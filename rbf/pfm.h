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


using namespace std;


struct SlotDir{
 int offset;//bytes from the start of page
 int length; //byte length of the record
 //SlotDir(int off, int len){ offset = off; length = len;}
};


class SlotDirectoryNode
{
private:
	int slotNum;
	SlotDir ptr;
//	SlotDirectoryNode *next;
	bool deleted;
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

	void setNext(SlotDirectoryNode *);
	SlotDirectoryNode *getNext();

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
	int pageNum;
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

	const int getEndPosition();//offset+length of last slot

	const int getOffset(int slot);
	const int getLength(int slot);

	const int getLastSlotNum();
	void setLastSlotNum(int);
	SlotDirectoryNode *getSlotNode(int num);
//	SlotDirectory *next;
};

class FreeBytes
{
private:
	int pageNum;
	int freeBytes;//left bytes of a page

protected:


public:
	FreeBytes();
	~FreeBytes();
	void setPageNum(int);
	void setFreeBytes(int);
	const int getPageNum();
	const int getFreeBytes();
	RC reduceFreeBytes(int);
	RC addFreeBytes(int);
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
	int pageMaxNum;

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

	RC appendSlot(int pageNum, int bytes); //after append a slot

	int lookforPage(int bytesNeed);
	int lookforSlot(int pageNum, int bytesNeed);
	RC reduceFreeBytes(int pageNum, int bytes);
	RC addFreeBytes(int pageNum, int bytes);

	int getOffset(int pageNum, int slotNum);//rid: pageId and slot id, return offset from the start of page
	int getLength(int pageNum, int slotNum);//rid: pageId and slot id, return offset from the start of page

	RC updateSlotLen(int pageNum, int slotNum, int len);
	int getEndPosition(int PageNum);//offset+ length of last slot in page: PageNum
	const int getLastSlotNum(int PageNum);

 };



 #endif
