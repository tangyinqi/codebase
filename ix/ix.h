#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"
#include "errcode.h"

#include <list>


#define IX_EOF (-1)  // end of the index scan

#define METADATA ".METADATA"

#define INITIAL_CAPACITY 8


//bool FileExists(const char *fileName);


union EntryKey{
	int key1;
	float key2;
	char *key3;
};

struct Entry{
	EntryKey key;
	RID rid;
};


class Bucket{
private:
	int bucketId;
	int last;
	//parameter: &numberOfPages
	int entryMaxNumber;
	vector<Entry> entrys;
	__gnu_cxx::hash_map<EntryKey,int> entryLookup;

public:
	Bucket(int id, int numberOfPages);
	~Bucket();

	RC addEntry(Entry entry);
	int getFirstEntryPostion(EntryKey key);
	int getBucketSize();
};

class PrimeBucket: public Bucket{
private:
	bool overFlow;
public:
	PrimeBucket(int, int);
	~PrimeBucket();
	bool isOverFlow();
	void setOverFlow();
	void resetOverFlow();
};

/*class OverflowBucket: public Bucket{

public:
	OverflowBucket(int, int);
//	bucketOverflow(){primePageId=-1;}
	~OverflowBucket();
};*/


class MetaData{
private:
	int initialCapacity;
	unsigned next;
	int level;
	int numberOfPagesEachBucket;
//	bool split;

public:
	MetaData(int initialCapacity, int next, int level, int numberOfPagesEachBucket);
	~MetaData();
	//increase level and next
	RC increaseNext();
	int getNext();
	int getLevel();
	int getBucketCapacity();
	int getInitialCapacity();
};

class OverFLowPages{
private:
	int numberOfPagesEachBucket;
	__gnu_cxx::hash_map<int, vector<Bucket*> > overFlowPages;
public:
	RC addOverFlowPage(int primePageId, Entry entry);
	OverFLowPages(int);
	~OverFLowPages();
};

class LinearHash{
private:
	MetaData *metaData;
	vector<PrimeBucket> primeBuckets;
	OverFLowPages *overFlowPages;
//	bool split; //flg for whether to split!!
public:

	LinearHash();
	~LinearHash();

	RC setMetaData(MetaData *meta);
	RC setPrimeBuckes(vector<PrimeBucket> primeBuckets);

	PrimeBucket getBucket(int bucketId);
	RC appendBucket(PrimeBucket bucket);

	unsigned hash(const Attribute &attribute, const void *key);

	RC insertEntry(int primePageId, Entry entry);
	RC deleteEntry(int primePageId, Entry entry);
	//triggered by some bucket is full,  strategy: round robin, controlled by "next"
	RC split();
};


class IX_ScanIterator;
class IXFileHandle;

class IndexManager {
 public:
  static IndexManager* instance();
  PagedFileManager *pfm = PagedFileManager::instance();

  LinearHash *linearHashing;
  // Create index file(s) to manage an index
  RC createFile(const string &fileName, const unsigned &numberOfPages);

  // Delete index file(s)
  RC destroyFile(const string &fileName);

  // Open an index and returns an IXFileHandle
  RC openFile(const string &fileName, IXFileHandle &ixFileHandle);

  // Close an IXFileHandle. 
  RC closeFile(IXFileHandle &ixfileHandle);

  // helper methods in openFile(), to initial and fulfil the field of LinearHash *linearHashing;
  RC readMetaData(const string &fileName, IXFileHandle &ixFileHandle);
  RC readIndex(const string &fileName, IXFileHandle &ixFileHandle);
  // helper methods in closeFile(), to store information of the linearHashing
  RC writeMetaData(const string &fileName, IXFileHandle &ixFileHandle);
  RC writeIndex(const string &fileName, IXFileHandle &ixFileHandle);


  // The following functions  are using the following format for the passed key value.
  //  1) data is a concatenation of values of the attributes
  //  2) For INT and REAL: use 4 bytes to store the value;
  //     For VarChar: use 4 bytes to store the length of characters, then store the actual characters.

  // Insert an entry to the given index that is indicated by the given IXFileHandle
  RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

  // Delete an entry from the given index that is indicated by the given IXFileHandle
  RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

  // scan() returns an iterator to allow the caller to go through the results
  // one by one in the range(lowKey, highKey).
  // For the format of "lowKey" and "highKey", please see insertEntry()
  // If lowKeyInclusive (or highKeyInclusive) is true, then lowKey (or highKey)
  // should be included in the scan
  // If lowKey is null, then the range is -infinity to highKey
  // If highKey is null, then the range is lowKey to +infinity
  
  // Initialize and IX_ScanIterator to supports a range search
  RC scan(IXFileHandle &ixfileHandle,
      const Attribute &attribute,
	  const void        *lowKey,
      const void        *highKey,
      bool        lowKeyInclusive,
      bool        highKeyInclusive,
      IX_ScanIterator &ix_ScanIterator);

  // Generate and return the hash value (unsigned) for the given key
  unsigned hash(const Attribute &attribute, const void *key);
  
  
  // Print all index entries in a primary page including associated overflow pages
  // Format should be:
  // Number of total entries in the page (+ overflow pages) : ?? 
  // primary Page No.??
  // # of entries : ??
  // entries: [xx] [xx] [xx] [xx] [xx] [xx]
  // overflow Page No.?? liked to [primary | overflow] page No.??
  // # of entries : ??
  // entries: [xx] [xx] [xx] [xx] [xx]
  // where [xx] shows each entry.
  RC printIndexEntriesInAPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const unsigned &primaryPageNumber);
  
  // Get the number of primary pages
  RC getNumberOfPrimaryPages(IXFileHandle &ixfileHandle, unsigned &numberOfPrimaryPages);

  // Get the number of all pages (primary + overflow)
  RC getNumberOfAllPages(IXFileHandle &ixfileHandle, unsigned &numberOfAllPages);
  
 protected:
  IndexManager   ();                            // Constructor
  ~IndexManager  ();                            // Destructor

 private:
  static IndexManager *_index_manager;
};


class IX_ScanIterator {
 public:
  IX_ScanIterator();  							// Constructor
  ~IX_ScanIterator(); 							// Destructor

  RC getNextEntry(RID &rid, void *key);  		// Get next matching entry
  RC close();             						// Terminate index scan
};


class IXFileHandle: public FileHandle {
public:


	// Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    IXFileHandle();  							// Constructor
    ~IXFileHandle(); 							// Destructor
    

private:
    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;
};

// print out the error message for a given return code
void IX_PrintError (RC rc);


#endif
