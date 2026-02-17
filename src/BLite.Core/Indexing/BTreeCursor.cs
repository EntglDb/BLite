using System.Buffers;
using BLite.Core.Storage;
using BLite.Bson;
using System.Collections.Generic;
using System;

namespace BLite.Core.Indexing;

internal sealed class BTreeCursor : IBTreeCursor
{
    private readonly BTreeIndex _index;
    private readonly ulong _transactionId;
    private readonly StorageEngine _storage;
    
    // State
    private byte[] _pageBuffer;
    private uint _currentPageId;
    private int _currentEntryIndex;
    private BTreeNodeHeader _currentHeader;
    private List<IndexEntry> _currentEntries;
    private bool _isValid;

    public BTreeCursor(BTreeIndex index, StorageEngine storage, ulong transactionId)
    {
        _index = index;
        _storage = storage;
        _transactionId = transactionId;
        _pageBuffer = ArrayPool<byte>.Shared.Rent(storage.PageSize);
        _currentEntries = new List<IndexEntry>();
        _isValid = false;
    }

    public IndexEntry Current 
    {
        get 
        {
            if (!_isValid) throw new InvalidOperationException("Cursor is not valid.");
            return _currentEntries[_currentEntryIndex];
        }
    }

    public bool MoveToFirst()
    {
        // Find left-most leaf
        var pageId = _index.RootPageId;
        while (true)
        {
            LoadPage(pageId);
            if (_currentHeader.IsLeaf) break;
            
            // Go to first child (P0)
            // Internal node format: [Header] [P0] [Entry1] ...
            var dataOffset = 32 + 20;
            pageId = BitConverter.ToUInt32(_pageBuffer.AsSpan(dataOffset, 4));
        }

        return PositionAtStart();
    }

    public bool MoveToLast()
    {
        // Find right-most leaf
        var pageId = _index.RootPageId;
        while (true)
        {
            LoadPage(pageId);
            if (_currentHeader.IsLeaf) break;

            // Go to last child (last pointer)
            // Iterate all entries to find last pointer
            // P0 is at 32+20 (4 bytes). Entry 0 starts at 32+20+4.
            
            // Wait, we need the last pointer.
            // P0 is at offset.
            // Then EncryCount entries: Key + Pointer.
            // We want the last pointer.
            
            // Re-read P0 just in case
            uint lastPointer = BitConverter.ToUInt32(_pageBuffer.AsSpan(32 + 20, 4));

            var offset = 32 + 20 + 4;
            for (int i = 0; i < _currentHeader.EntryCount; i++)
            {
                var keyLen = BitConverter.ToInt32(_pageBuffer.AsSpan(offset, 4));
                offset += 4 + keyLen;
                lastPointer = BitConverter.ToUInt32(_pageBuffer.AsSpan(offset, 4));
                offset += 4;
            }
            pageId = lastPointer;
        }

        return PositionAtEnd();
    }

    public bool Seek(IndexKey key)
    {
        // Use Index to find leaf
        var leafPageId = _index.FindLeafNode(key, _transactionId);
        LoadPage(leafPageId);
        ParseEntries();

        // Binary search in entries
        var idx = _currentEntries.BinarySearch(new IndexEntry(key, default(DocumentLocation)));
        
        if (idx >= 0)
        {
            // Found exact match
            _currentEntryIndex = idx;
            _isValid = true;
            return true;
        }
        else
        {
            // Not found, ~idx is the next larger value
            _currentEntryIndex = ~idx;
            
            if (_currentEntryIndex < _currentEntries.Count)
            {
                _isValid = true;
                return false; // Positioned at next greater
            }
            else
            {
                // Key is larger than max in this page, move to next page
                if (_currentHeader.NextLeafPageId != 0)
                {
                    LoadPage(_currentHeader.NextLeafPageId);
                    ParseEntries();
                    _currentEntryIndex = 0;
                    if (_currentEntries.Count > 0)
                    {
                        _isValid = true;
                        return false;
                    }
                }
                
                // End of index
                _isValid = false;
                return false;
            }
        }
    }

    public bool MoveNext()
    {
        if (!_isValid) return false;

        _currentEntryIndex++;
        if (_currentEntryIndex < _currentEntries.Count)
        {
            return true;
        }

        // Move to next page
        if (_currentHeader.NextLeafPageId != 0)
        {
            LoadPage(_currentHeader.NextLeafPageId);
            return PositionAtStart();
        }

        _isValid = false;
        return false;
    }

    public bool MovePrev()
    {
        if (!_isValid) return false;

        _currentEntryIndex--;
        if (_currentEntryIndex >= 0)
        {
            return true;
        }

        // Move to prev page
        if (_currentHeader.PrevLeafPageId != 0)
        {
            LoadPage(_currentHeader.PrevLeafPageId);
            return PositionAtEnd();
        }

        _isValid = false;
        return false;
    }

    private void LoadPage(uint pageId)
    {
        if (_currentPageId == pageId && _pageBuffer != null) return;

        _index.ReadPage(pageId, _transactionId, _pageBuffer);
        _currentPageId = pageId;
        _currentHeader = BTreeNodeHeader.ReadFrom(_pageBuffer.AsSpan(32));
    }

    private void ParseEntries()
    {
        // Helper to parse entries from current page buffer
        // (Similar to BTreeIndex.ReadLeafEntries)
        _currentEntries.Clear();
        var dataOffset = 32 + 20;

        for (int i = 0; i < _currentHeader.EntryCount; i++)
        {
            // Read Key
            var keyLen = BitConverter.ToInt32(_pageBuffer.AsSpan(dataOffset, 4));
            var keyData = new byte[keyLen];
            _pageBuffer.AsSpan(dataOffset + 4, keyLen).CopyTo(keyData);
            var key = new IndexKey(keyData);
            dataOffset += 4 + keyLen;

            // Read Location
            var location = DocumentLocation.ReadFrom(_pageBuffer.AsSpan(dataOffset, DocumentLocation.SerializedSize));
            dataOffset += DocumentLocation.SerializedSize;

            _currentEntries.Add(new IndexEntry(key, location));
        }
    }

    private bool PositionAtStart()
    {
        ParseEntries();
        if (_currentEntries.Count > 0)
        {
            _currentEntryIndex = 0;
            _isValid = true;
            return true;
        }
        else
        {
            // Empty page? Should not happen in helper logic unless root leaf is empty
            _isValid = false;
            return false;
        }
    }

    private bool PositionAtEnd()
    {
        ParseEntries();
        if (_currentEntries.Count > 0)
        {
            _currentEntryIndex = _currentEntries.Count - 1;
            _isValid = true;
            return true;
        }
        else
        {
             _isValid = false;
             return false;
        }
    }

    public void Dispose()
    {
        if (_pageBuffer != null)
        {
            ArrayPool<byte>.Shared.Return(_pageBuffer);
            _pageBuffer = null!;
        }
    }
}
