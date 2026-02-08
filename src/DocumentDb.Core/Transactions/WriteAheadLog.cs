namespace DocumentDb.Core.Transactions;

/// <summary>
/// WAL record types
/// </summary>
public enum WalRecordType : byte
{
    Begin = 1,
    Write = 2,
    Commit = 3,
    Abort = 4,
    Checkpoint = 5
}

/// <summary>
/// Write-Ahead Log (WAL) for durability and recovery.
/// All changes are logged before being applied.
/// </summary>
public sealed class WriteAheadLog : IDisposable
{
    private readonly string _walPath;
    private FileStream? _walStream;
    private readonly object _lock = new();
    private bool _disposed;

    public WriteAheadLog(string walPath)
    {
        _walPath = walPath ?? throw new ArgumentNullException(nameof(walPath));
        
        _walStream = new FileStream(
            _walPath,
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.None,  // Exclusive access like PageFile
            bufferSize: 64 * 1024); // 64KB buffer for better sequential write performance
        // REMOVED FileOptions.WriteThrough for SQLite-style lazy checkpointing
        // Durability is ensured by explicit Flush() calls
    }

    /// <summary>
    /// Writes a begin transaction record
    /// </summary>
    public void WriteBeginRecord(ulong transactionId)
    {
        lock (_lock)
        {
            Span<byte> buffer = stackalloc byte[17];  // type(1) + txnId(8) + timestamp(8)
            buffer[0] = (byte)WalRecordType.Begin;
            BitConverter.TryWriteBytes(buffer[1..9], transactionId);
            BitConverter.TryWriteBytes(buffer[9..17], DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            
            _walStream!.Write(buffer);
        }
    }

    /// <summary>
    /// Writes a commit record
    /// </summary>
    public void WriteCommitRecord(ulong transactionId)
    {
        lock (_lock)
        {
            Span<byte> buffer = stackalloc byte[17];  // type(1) + txnId(8) + timestamp(8)
            buffer[0] = (byte)WalRecordType.Commit;
            BitConverter.TryWriteBytes(buffer[1..9], transactionId);
            BitConverter.TryWriteBytes(buffer[9..17], DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            
            _walStream!.Write(buffer);
        }
    }

    /// <summary>
    /// Writes an abort record
    /// </summary>
    public void WriteAbortRecord(ulong transactionId)
    {
        lock (_lock)
        {
            Span<byte> buffer = stackalloc byte[17];  // type(1) + txnId(8) + timestamp(8)
            buffer[0] = (byte)WalRecordType.Abort;
            BitConverter.TryWriteBytes(buffer[1..9], transactionId);
            BitConverter.TryWriteBytes(buffer[9..17], DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            
            _walStream!.Write(buffer);
        }
    }

    /// <summary>
    /// Writes a data modification record
    /// </summary>
    public void WriteDataRecord(ulong transactionId, uint pageId, ReadOnlySpan<byte> afterImage)
    {
        lock (_lock)
        {
            // Header: type(1) + txnId(8) + pageId(4) + afterSize(4) = 17 bytes
            var headerSize = 17;
            var totalSize = headerSize + afterImage.Length;
            
            // OPTIMIZATION: Use ArrayPool instead of allocating new array
            var buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(totalSize);
            try
            {
                buffer[0] = (byte)WalRecordType.Write;
                BitConverter.TryWriteBytes(buffer.AsSpan(1, 8), transactionId);
                BitConverter.TryWriteBytes(buffer.AsSpan(9, 4), pageId);
                BitConverter.TryWriteBytes(buffer.AsSpan(13, 4), afterImage.Length);
                
                afterImage.CopyTo(buffer.AsSpan(headerSize));
                
                _walStream!.Write(buffer.AsSpan(0, totalSize));
            }
            finally
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
            }
        }
    }

    /// <summary>
    /// Flushes all buffered writes to disk
    /// </summary>
    public void Flush()
    {
        lock (_lock)
        {
            _walStream?.Flush(flushToDisk: true);
        }
    }

    /// <summary>
    /// Gets the current size of the WAL file in bytes
    /// </summary>
    public long GetCurrentSize()
    {
        lock (_lock)
        {
            return _walStream?.Length ?? 0;
        }
    }

    /// <summary>
    /// Truncates the WAL file (removes all content).
    /// Should only be called after successful checkpoint.
    /// </summary>
    public void Truncate()
    {
        lock (_lock)
        {
            if (_walStream != null)
            {
                _walStream.SetLength(0);
                _walStream.Position = 0;
                _walStream.Flush(flushToDisk: true);
            }
        }
    }

    /// <summary>
    /// Reads all WAL records (for recovery)
    /// </summary>
    public List<WalRecord> ReadAll()
    {
        lock (_lock)
        {
            var records = new List<WalRecord>();
            
            if (_walStream == null || _walStream.Length == 0)
                return records;

            _walStream.Position = 0;
            
            // Allocate buffers outside loop to avoid CA2014 warning
            Span<byte> headerBuf = stackalloc byte[16];
            Span<byte> dataBuf = stackalloc byte[12];
            
            while (_walStream.Position < _walStream.Length)
            {
                var typeByte = _walStream.ReadByte();
                if (typeByte == -1) break;
                
                var type = (WalRecordType)typeByte;
                
                // Check for invalid record type (file padding or corruption)
                if (typeByte == 0 || !Enum.IsDefined(typeof(WalRecordType), type))
                {
                    // Reached end of valid records (file may have padding)
                    break;
                }
                
                WalRecord record;
                
                switch (type)
                {
                    case WalRecordType.Begin:
                    case WalRecordType.Commit:
                    case WalRecordType.Abort:
                        // Read common fields (txnId + timestamp = 16 bytes)
                        var bytesRead = _walStream.Read(headerBuf);
                        if (bytesRead < 16)
                        {
                            // Incomplete record, stop reading
                            return records;
                        }
                        
                        var txnId = BitConverter.ToUInt64(headerBuf[0..8]);
                        var timestamp = BitConverter.ToInt64(headerBuf[8..16]);
                        
                        record = new WalRecord 
                        { 
                            Type = type, 
                            TransactionId = txnId, 
                            Timestamp = timestamp 
                        };
                        break;
                        
                    case WalRecordType.Write:
                        // Write records have different format: txnId(8) + pageId(4) + afterSize(4)
                        // Read txnId + pageId + afterSize = 16 bytes
                        bytesRead = _walStream.Read(headerBuf);
                        if (bytesRead < 16) 
                        {
                            // Incomplete write record header, stop reading
                            return records;
                        }
                        
                        txnId = BitConverter.ToUInt64(headerBuf[0..8]);
                        var pageId = BitConverter.ToUInt32(headerBuf[8..12]);
                        var afterSize = BitConverter.ToInt32(headerBuf[12..16]);
                        
                        // Validate afterSize to prevent overflow or corruption
                        if (afterSize < 0 || afterSize > 100 * 1024 * 1024) // Max 100MB per record
                        {
                            // Corrupted size, stop reading
                            return records;
                        }
                        
                        var afterImage = new byte[afterSize];
                        
                        // Read afterImage
                        if (_walStream.Read(afterImage) < afterSize)
                        {
                            // Incomplete after image, stop reading
                            return records;
                        }
                        
                        record = new WalRecord
                        {
                            Type = type,
                            TransactionId = txnId,
                            Timestamp = 0, // Write records don't have timestamp
                            PageId = pageId,
                            AfterImage = afterImage
                        };
                        break;
                        
                    default:
                        // Unknown record type, stop reading
                        return records;
                }
                
                records.Add(record);
            }

            return records;
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        lock (_lock)
        {
            _walStream?.Dispose();
            _disposed = true;
        }

        GC.SuppressFinalize(this);
    }
}

/// <summary>
/// Represents a WAL record.
/// Implemented as struct for memory efficiency.
/// </summary>
public struct WalRecord
{
    public WalRecordType Type { get; set; }
    public ulong TransactionId { get; set; }
    public long Timestamp { get; set; }
    public uint PageId { get; set; }
    public byte[]? AfterImage { get; set; }
}
