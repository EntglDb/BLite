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
            bufferSize: 4096,
            FileOptions.WriteThrough); // Ensures writes go to disk immediately
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
    public void WriteDataRecord(ulong transactionId, uint pageId, ReadOnlySpan<byte> beforeImage, ReadOnlySpan<byte> afterImage)
    {
        lock (_lock)
        {
            // Header: type(1) + txnId(8) + pageId(4) + beforeSize(4) + afterSize(4) = 21 bytes
            var headerSize = 21;
            var totalSize = headerSize + beforeImage.Length + afterImage.Length;
            
            var buffer = new byte[totalSize];
            buffer[0] = (byte)WalRecordType.Write;
            BitConverter.TryWriteBytes(buffer.AsSpan(1, 8), transactionId);
            BitConverter.TryWriteBytes(buffer.AsSpan(9, 4), pageId);
            BitConverter.TryWriteBytes(buffer.AsSpan(13, 4), beforeImage.Length);
            BitConverter.TryWriteBytes(buffer.AsSpan(17, 4), afterImage.Length);
            
            beforeImage.CopyTo(buffer.AsSpan(headerSize));
            afterImage.CopyTo(buffer.AsSpan(headerSize + beforeImage.Length));
            
            _walStream!.Write(buffer);
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
                
                // Read common fields (txnId + timestamp = 16 bytes)
                var bytesRead = _walStream.Read(headerBuf);
                if (bytesRead < 16) break; // Incomplete record
                
                var txnId = BitConverter.ToUInt64(headerBuf[0..8]);
                var timestamp = BitConverter.ToInt64(headerBuf[8..16]);
                
                WalRecord record;
                
                switch (type)
                {
                    case WalRecordType.Begin:
                    case WalRecordType.Commit:
                    case WalRecordType.Abort:
                        record = new WalRecord 
                        { 
                            Type = type, 
                            TransactionId = txnId, 
                            Timestamp = timestamp 
                        };
                        break;
                        
                    case WalRecordType.Write:
                        // Read data record specific fields (pageId + beforeSize + afterSize = 12 bytes)
                        bytesRead = _walStream.Read(dataBuf);
                        if (bytesRead < 12) 
                        {
                            // Incomplete write record, stop reading
                            return records;
                        }
                        
                        var pageId = BitConverter.ToUInt32(dataBuf[0..4]);
                        var beforeSize = BitConverter.ToInt32(dataBuf[4..8]);
                        var afterSize = BitConverter.ToInt32(dataBuf[8..12]);
                        
                        var beforeImage = new byte[beforeSize];
                        var afterImage = new byte[afterSize];
                        
                        if (_walStream.Read(beforeImage) < beforeSize)
                        {
                            // Incomplete before image, stop reading
                            return records;
                        }
                        if (_walStream.Read(afterImage) < afterSize)
                        {
                            // Incomplete after image, stop reading
                            return records;
                        }
                        
                        record = new WalRecord
                        {
                            Type = type,
                            TransactionId = txnId,
                            Timestamp = timestamp,
                            PageId = pageId,
                            BeforeImage = beforeImage,
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
    public byte[]? BeforeImage { get; set; }
    public byte[]? AfterImage { get; set; }
}
