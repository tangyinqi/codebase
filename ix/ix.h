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
class IX_ScanIterator;
class IXFileHandle;

//PagedFileManager *pfm = PagedFileManager::instance();

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
protected:
	int bucketId;
	int last;
	bool overFlow;
	//parameter: &numberOfPages
	int entryMaxNumber;
	vector<Entry> entrys;
	unordered_map<int,int> entryLookup1;
	unordered_map<float,int> entryLookup2;
	unordered_map<string,int> entryLookup3;

public:
	Bucket();
	Bucket(int id);
	~Bucket();

	void setId(int id){ bucketId = id; }
//	void setCapacity(int numberOfPages){ entryMaxNumber = numberOfPages;}

	RC addEntry(Entry entry, int type);
	Entry getEntry(int id);

	bool isEmpty();
	int getBucketId();
	int getBucketCapacity();
	int getLast();

	int getFirstEntryPostion(const Attribute &attribute, const void *key);
	RC deleteEntry(const Attribute &attribute, const void *key, const RID &rid);

	RC rangeScan(const Attribute &attribute,
		  const void        *lowKey,
		  const void        *highKey,
		  bool        lowKeyInclusive,
		  bool        highKeyInclusive,
		  IX_ScanIterator &ix_ScanIterator);

	RC equalityScan(const Attribute &attribute, const void *Key,
			IX_ScanIterator &ix_ScanIterator);

	int getBucketSize();

	bool isOverFlow();
	void setOverFlow();
	void resetOverFlow();
	void print(const Attribute &attribute);
};

class PrimeBucket: public Bucket{
private:
	bool overFlow;
public:
	PrimeBucket(int id);//:Bucket(id, numberOfPages){ overFlow = false;}
	~PrimeBucket();
	int getBucketSize();//{return Bucket::getBucketSize();}
	Entry getEntry(int id);
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
	int numberOfPages;
	unsigned next;
	int level;
	int type;//-1 undefined, {0,1,2}

//	bool split;

public:
	MetaData(int initialCapacity, unsigned next, int level, int numberOfPages);
	~MetaData();
	//increase level and next
	RC increaseNext();
	RC decreaseNext();

	const unsigned getNext();
	const int getLevel();
	const int getPageNum();
	const int getInitialCapacity();
	const int getType();
	void setType(int Type);
};

class OverFLowPages{
private:
//	int numberOfPagesEachBucket;
	unordered_map<int, vector<Bucket*> > overFlowPages;
public:
	RC addOverFlowPage(int primePageId, Entry entry, const Attribute &attribute);
	RC buildOverFlowHashMap(int primePageId, vector<Bucket*> buckets);
	vector<int>* getKeySet();
	vector<Bucket*> getValue(int primePageId);
	void deleteValue(int primePageId);

	OverFLowPages();
	~OverFLowPages();
};

class LinearHash{
private:
	MetaData *metaData;
	vector<Bucket *> primeBuckets;
	int primeBucketNumber;
	OverFLowPages *overFlowPages;
	bool splitTriggerred; //flg for whether to split!!

public:

	LinearHash();
	~LinearHash();

	RC setMetaData(MetaData *meta);
	MetaData *getMetaData(){return metaData;}

	OverFLowPages *getOverFlowPages(){return overFlowPages;}
	RC setOverFlowPages(OverFLowPages *overFlowPages);

	RC setPrimeBuckes(vector<Bucket *> primeBuckets);
	Bucket *getBucket(unsigned bucketId);
	int getPrimeBucketNumber(){return primeBucketNumber;}
	//this function function can be used in both building linearHashing(), and
	RC appendBucket(Bucket *bucket);

	unsigned hash(const Attribute &attribute, const void *key, int level);
	unsigned hash(const Attribute &attribute, EntryKey key, int level);

	unsigned lookforBucket(const Attribute &attribute, const void *key);

	bool isSplitTriggerred();
	RC insertEntry(unsigned primePageId, const Attribute &attribute, Entry entry);
	RC deleteEntry(unsigned primePageId, const Attribute &attribute, const void *key, const RID &rid);
	//triggered by some bucket is full,  strategy: round robin, controlled by "next"
	RC split(const Attribute &attribute);

	RC rangeScan(const Attribute &attribute,
			  const void        *lowKey,
			  const void        *highKey,
			  bool        lowKeyInclusive,
			  bool        highKeyInclusive,
			  IX_ScanIterator &ix_ScanIterator);

	RC equalityScan(const Attribute &attribute, const void *Key,
			IX_ScanIterator &ix_ScanIterator);

};


class IndexManager {
//private:


 public:
  static IndexManager* instance();


  // Create index file(s) to manage an index
  RC createFile(const string &fileName, const unsigned &numberOfPages);

  // Delete index file(s)
  RC destroyFile(const string &fileName);

  // Open an index and returns an IXFileHandle
  RC openFile(const string &fileName, IXFileHandle &ixFileHandle);

  // Close an IXFileHandle. 
  RC closeFile(IXFileHandle &ixfileHandle);


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
private:
	int pos;
	int eof;
	vector<Entry> entries;
	int type;

 public:
  IX_ScanIterator();  							// Constructor
  ~IX_ScanIterator(); 							// Destructor

  void setType(int TYPE){type = TYPE;}
  RC addEntry(const Entry entry);

  RC getNextEntry(RID &rid, void *key);  		// Get next matching entry
  RC close();             						// Terminate index scan
};



class XFileHandle
{
private:
	FILE *pFile;
	unsigned pageMaxNum;

    char pageBuffer[100][PAGE_SIZE];//100 page buffer
    int topBuffer;
    bool isClean[100];//whether the page is dirty or clean
    unordered_map<unsigned, unsigned> pageToBuffer;//pageNum, bufferId
    unordered_map<unsigned, unsigned> bufferToPage;//bufferId, pageNum

public:
    XFileHandle();                                                    // Default constructor
//    FileHandle(const char *fileName);

    ~XFileHandle();                                                   // Destructor
    RC closeHandle();
    RC setHandle(FILE *p);

    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();                                        // Get the number of pages in the file

};

class IXFileHandle{
private:
	FILE *pIndex;
	FILE *pMetaData;
	LinearHash *linearHashing;
	int pageMaxNum;

public:
	//IXFileHandle():FileHandle(){}
	RC setIndexHandle(FILE *p);
	RC setMedatDataHandle(FILE *p);
	RC closeHandle();
	// helper methods in openFile(), to initial and fulfill the field of LinearHash *linearHashing;
	RC readMetaData();
	RC readPrimePages();
	RC readFlowPages();

	// helper methods in closeFile(), to store information of the linearHashing
	RC writeMetaData();
	RC writePrimePages();
	RC writeFlowPages();

    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page

	LinearHash *getLinearHash();
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
