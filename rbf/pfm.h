#ifndef _pfm_h_
#define _pfm_h_

typedef int RC;
typedef unsigned PageNum;

#define PAGE_SIZE 4096

#define INFO ".info"
//#define SLOTDIRECTORY ".slotdir"

#include <stdio.h>
#include <list>
#include <string>
//#include <stdlib.h>
#include <vector>
#include "cassert"
#include <sys/stat.h>

using namespace std;

bool FileExists(const char *fileName);

struct SlotDir{
 int offset;//bytes from the start of page
 int length; //byte length of the record
 //SlotDir(int off, int len){ offset = off; length = len;}
};

// Record ID
typedef struct
{
  unsigned pageNum;
  unsigned slotNum;
} RID;


class SlotDirectoryNode
{
private:
	int slotNum;
	SlotDir ptr;
	RID next;
	bool deleted;
	bool tombStone;
protected:

	~SlotDirectoryNode();
public:
	SlotDirectoryNode();
	void setSlotNum(int);
	int getSlotNum();

	void setSlotDir(int offset, int len);

	const int getSlotOffset();
	const int getSlotLength();

	RC updateSlotLength(int len);
	RC updateSlotOffset(int offset);

	RC setTombStoneRID(const RID &rid);
	RC getTombStoneRID(RID &rid);

	RC setTombStone();
	const bool isTombStone();

	RC setDelete();
	RC setUsed();
	const bool isDeleted();

//	void setSlotLength(int);

};

//pageNum start from 1
//slot num start from 1
class SlotDirectory
{
private:
	unsigned pageNum;
	int lastSlotNum;//start from 0;
	vector<SlotDirectoryNode *> psList;
//	SlotDirectoryNode *curr; //the last element

protected:

	~SlotDirectory();

public:
	SlotDirectory();
	void setPageNum(int);
	const int getPageNum();
	RC appendSlot(int offset, int length);

	RC appendSlotNode(SlotDirectoryNode *);//read info

	const SlotDir getSlotDir(int slotNum);
	RC deleteSlot(int slotNum);


	const int lookforSlotNum(int bytesNeed);//-1:no deleted slot has required space
	RC updateSlotLength(int slotNum, int len);
	RC updateSlotOffset(int slotNum, int off);

	const int getEndPosition();//offset+length of last slot

	const int getOffset(int slot);
	const int getLength(int slot);

	const int getLastSlotNum();
	void setLastSlotNum(int);
	SlotDirectoryNode *getSlotNode(int num);

	RC setSlotDeleted(int slotNum);
	const bool isSlotDeleted(int slotNum);

	RC setSlotTombStone(int slotNum);
	const bool isSlotTombSone(int slotNum);

	RC setSlotTombStoneRID(int slotNum, const RID &rid);
	RC getSlotTombStoneRID(int slotNum, RID &rid);

//	SlotDirectory *next;
};

class FreeBytes
{
private:
	unsigned pageNum;
	int freeBytes;//left bytes of a page

protected:


public:
	FreeBytes();
	~FreeBytes();
	void setPageNum(unsigned);
	void setFreeBytes(int);
	const unsigned getPageNum();
	const int getFreeBytes();
	RC reduceFreeBytes(int);
	RC addFreeBytes(int);
	RC reset();
//	FreeBytes *next;
};

class FileHandle;

class PagedFileManager
{

public:
    static PagedFileManager* instance();                     // Access to the _pf_manager instance

    RC createFile    (const char *fileName);                         // Create a new file
    RC destroyFile   (const char *fileName);                         // Destroy a file
    RC openFile      (const char *fileName, FileHandle &fileHandle); // Open a file
    RC closeFile     (FileHandle &fileHandle);                       // Close a file

protected:
    PagedFileManager();                                   // Constructor
    ~PagedFileManager();                                  // Destructor

private:

    static PagedFileManager *_pf_manager;
};


class FileHandle
{
private:
	FILE *pFile;
	FILE *pageInfo;
	unsigned pageMaxNum;

    vector<FreeBytes *> fbList;
//    FreeBytes * curr;

    vector<SlotDirectory *> pageSlotDirectory;


public:
    FileHandle();                                                    // Default constructor
//    FileHandle(const char *fileName);

    ~FileHandle();                                                   // Destructor
    RC closeHandle();
    RC setHandle(FILE *p);
    RC setPageInfo(FILE *p);
    RC savePageInfo();
    RC readPageInfo();

    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();                                        // Get the number of pages in the file

	RC appendToPageFreeSpace();//after FileHandle::appendPage(), so that the pageNum ++!!!

	RC appendToSlotDirectory();//after FileHandle::appendPage(), append a slot directory

	RC appendSlot(unsigned pageNum, int bytes); //after append a slot

	unsigned lookforPage(int bytesNeed);
	unsigned lookforSlot(unsigned pageNum, int bytesNeed);

	RC setSlotDeleted(unsigned pageNum, int slotNum);
	const bool isSlotDeleted(unsigned pageNum, int slotNum);

	RC setSlotTombStone(unsigned pageNum, int slotNum);
	const bool isSlotTombStone(unsigned pageNum, int slotNum);

	RC setSlotTombStoneRID(unsigned pageNum, int slotNum, const RID &rid);
	RC getSlotTombStoneRID(unsigned pageNum, int slotNum, RID &rid);


	RC reduceFreeBytes(unsigned pageNum, int bytes);
	RC addFreeBytes(unsigned pageNum, int bytes);
	const int getFreeBytes(unsigned pageNum);
	RC reset(unsigned pageNum);

	int getOffset(unsigned pageNum, int slotNum);//rid: pageId and slot id, return offset from the start of page
	int getLength(unsigned pageNum, int slotNum);//rid: pageId and slot id, return offset from the start of page

	RC updateSlotLen(unsigned pageNum, int slotNum, int len);
	RC updateSlotOffset(unsigned pageNum, int slotNum, int len);

	int getEndPosition(unsigned PageNum);//offset+ length of last slot in page: PageNum
	const int getLastSlotNum(unsigned PageNum);

 };



 #endif
