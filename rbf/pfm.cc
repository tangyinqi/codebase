#include "pfm.h"
#include <iostream>
#include <fstream>
//#include <string>
//#include <stdlib.h>
#include <stdio.h>

#include <unistd.h>
//#include <assert.h>
#include <string>

/******************   helper method   **********************/
// Check if a file exists
bool FileExists(const char *fileName)
{
    struct stat stFileInfo;

    if(stat(fileName, &stFileInfo) == 0) return true;
    else return false;
}



/*******************    implementation of class PageSlotDirectory   *************************/

SlotDirectoryNode::SlotDirectoryNode()
{
	slotNum = -1;
	ptr.length=-1;
	ptr.offset=-1;
	next.pageNum = -1;
	next.slotNum = -1;
	deleted = false;
	tombStone = false;
}

SlotDirectoryNode::~SlotDirectoryNode(){
	slotNum = -1;
	//if(ptr){
	//delete(ptr);
	//}

}

void SlotDirectoryNode::setSlotNum(int num)
{
	slotNum = num;
}

int SlotDirectoryNode::getSlotNum()
{
	return slotNum;
}

const int SlotDirectoryNode::getSlotOffset()
{
	return ptr.offset;
}

const int SlotDirectoryNode::getSlotLength()
{
	return ptr.length;
}

RC SlotDirectoryNode::updateSlotLength(int len)
{
	ptr.length = len;
	return 0;
}

RC SlotDirectoryNode::updateSlotOffset(int offset)
{
	ptr.offset = offset;
	return 0;
}

void SlotDirectoryNode::setSlotDir(int off, int len)
{
	printf("......entering setSlotDir()......\n");
	ptr.offset = off;
	ptr.length = len;
	printf("offset:%d, length:%d\n", ptr.offset, ptr.length);
}

RC SlotDirectoryNode::setTombStone()
{
	tombStone = true;
	return 0;
}

const bool SlotDirectoryNode::isTombStone()
{
	return tombStone;
}

RC SlotDirectoryNode::setTombStoneRID(const RID &rid)
{
	next.pageNum = rid.pageNum;
	next.slotNum = rid.slotNum;
	return 0;
}

RC SlotDirectoryNode::getTombStoneRID(RID &rid)
{
	rid.pageNum = next.pageNum;
	rid.slotNum = next.slotNum;
	return 0;
}

RC SlotDirectoryNode::setDelete()
{
	deleted = true;
	return 0;
}

RC SlotDirectoryNode::setUsed()
{
	deleted = false;
	return 0;
}

const bool SlotDirectoryNode::isDeleted()
{
	return deleted;
}

/*******************    implementation of class SlotDirectory   *************************/

SlotDirectory::SlotDirectory()
{
//	printf("clss SlotDirectory size: %ld\n", sizeof(SlotDirectory));
	pageNum = -1;
	//psList = NULL;
//	curr = NULL;
	lastSlotNum = -1;
}

SlotDirectory::~SlotDirectory()
{
	pageNum = -1;
	if(psList.size() != 0){
		psList.clear();
	}
//	if(curr){
//		free(curr);
//	}
}


void SlotDirectory::setPageNum(int num)
{
	pageNum = num;
}

const int SlotDirectory::getPageNum()
{
	return pageNum;
}

RC SlotDirectory::appendSlot(int ooffset, int len)
{

	lastSlotNum++;

	SlotDirectoryNode *psDir = new SlotDirectoryNode();

	//SlotDir slotDir = SlotDir(ooffset, len);

	psDir->setSlotNum(lastSlotNum);

	psDir->setSlotDir(ooffset, len);


	psList.push_back(psDir);


//	if(curr != NULL){
//		curr->setNext(psDir);
//	}
//	curr = psDir;

	return 0;
}

RC SlotDirectory::appendSlotNode(SlotDirectoryNode *p)
{
	psList.push_back(p);
	return 0;
}

RC SlotDirectory::deleteSlot(int slotNum)
{
	if(slotNum > psList.size()-1)
		return -16;
	SlotDirectoryNode *sptr = psList[slotNum];
	return sptr->setDelete();
}

const int SlotDirectory::getLastSlotNum()
{
	return lastSlotNum;
}

//return the slot number which has available space for bytes
const int SlotDirectory::lookforSlotNum(int bytesNeed)
{
	SlotDirectoryNode *p;
	for(int i=0; i < psList.size(); i++)
	{
		p = psList[i];
		if(p->isDeleted() && (p->getSlotLength() >= bytesNeed))
		{
			return i;
		}
	}
	return -1;
}

RC SlotDirectory::updateSlotLength(int slotNum, int len)
{
	if(slotNum > lastSlotNum)return -27;
	SlotDirectoryNode * p = psList[slotNum];
	p->updateSlotLength(len);
	return 0;

}

RC SlotDirectory::updateSlotOffset(int slotNum, int off)
{
	if(slotNum > lastSlotNum)return -27;
	SlotDirectoryNode * p = psList[slotNum];
	p->updateSlotOffset(off);
	return 0;

}

const int SlotDirectory::getEndPosition()
{
	if(lastSlotNum == -1)return 0;
	SlotDirectoryNode * p = psList[lastSlotNum];
	int offset = p->getSlotOffset() + p->getSlotLength();
//	psList[lastSlotNum]
	return offset;
}

//the offset from the start of the current page
const int SlotDirectory::getOffset(int slotNum)
{
	if(slotNum > lastSlotNum)
		return -1;

	SlotDirectoryNode * ptr = psList[slotNum];
	return ptr->getSlotOffset();
}

const int SlotDirectory::getLength(int slotNum)
{
	if(slotNum > lastSlotNum)
		return -1;

	SlotDirectoryNode * ptr = psList[slotNum];
	return ptr->getSlotLength();
}

void SlotDirectory::setLastSlotNum(int num)
{
	lastSlotNum = num;
}

SlotDirectoryNode *SlotDirectory::getSlotNode(int num)
{
	if(num > lastSlotNum)return NULL;
	SlotDirectoryNode *p = psList[num];
	return p;
}

RC SlotDirectory::setSlotDeleted(int slotNum)
{
	if(slotNum > lastSlotNum)return -22;
	SlotDirectoryNode *p = psList[slotNum];
	return p->setDelete();
}

const bool SlotDirectory::isSlotDeleted(int slotNum)
{
	if(slotNum > lastSlotNum)return -22;
	SlotDirectoryNode *p = psList[slotNum];
	return p->isDeleted();
}

RC SlotDirectory::setSlotTombStone(int slotNum)
{
	if(slotNum > lastSlotNum)return -22;
	SlotDirectoryNode *p = psList[slotNum];
	return p->setTombStone();
}

const bool SlotDirectory::isSlotTombSone(int slotNum)
{
	if(slotNum > lastSlotNum)return -22;
	SlotDirectoryNode *p = psList[slotNum];
	return p->isTombStone();
}

RC SlotDirectory::setSlotTombStoneRID(int slotNum, const RID &rid)
{
	if(slotNum > lastSlotNum)return -22;
	SlotDirectoryNode *p = psList[slotNum];
	return p->setTombStoneRID(rid);
}

RC SlotDirectory::getSlotTombStoneRID(int slotNum, RID &rid)
{
	if(slotNum > lastSlotNum)return -22;
	SlotDirectoryNode *p = psList[slotNum];
	if(!p->isTombStone()){
		return -23;
	}
	return p->getTombStoneRID(rid);
}

/*
const SlotDir SlotDirectory::getSlotDir(int slotNum){
	PageSlotDirectory *psDir = psList;

	while(psDir!= NULL)
	{
		if(psDir->getSlotNum() != slotNum){
			psDir = psDir->getNext();
		}else{
			int offset = psDir->getSlotOffset();
			int length = psDir->getSlotLength();
			SlotDir slotDir = SlotDir(offset, length);
			return slotDir;
		}
	}
	SlotDir slotDir = SlotDir(-1, -1);
	return slotDir;

}*/

/*******************    implementation of class FreeBytes   *************************/

FreeBytes::FreeBytes()
{
	//next = NULL;
}

FreeBytes::~FreeBytes(){

	//delete next;
}

void FreeBytes::setPageNum(unsigned num){
	pageNum = num;
}

void FreeBytes::setFreeBytes(int fbytes){
	freeBytes = fbytes;
}

const unsigned FreeBytes::getPageNum()
{
	return pageNum;
}

const int FreeBytes::getFreeBytes()
{
	return freeBytes;
}

RC FreeBytes::reduceFreeBytes(int bytes){
	if(bytes > freeBytes) {
		printf("freeBytes=%d , bytes=%d \n", freeBytes, bytes);
		return -17;
	}
	freeBytes -= bytes;
	return 0;
}

RC FreeBytes::addFreeBytes(int bytes){
	if((bytes + freeBytes) > PAGE_SIZE) return -19;
	freeBytes += bytes;
	return 0;
}

RC FreeBytes::reset(){
	freeBytes = PAGE_SIZE;
	return 0;
}

/*******************    implementation of class PagedFileManager   *************************/

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{

}


PagedFileManager::~PagedFileManager()
{

}



RC PagedFileManager::createFile(const char *fileName)
{

	if(FileExists(fileName)) return -1;

	FILE *pFile = fopen(fileName, "w");
	if(pFile!=NULL){
		int rc = fclose(pFile);
		assert(rc ==0);
	}

	pFile = fopen(string(fileName).append(INFO).c_str(), "w");
	if (pFile != NULL) {
		int rc = fclose(pFile);
		assert(rc == 0);
	}

    return 0;
}


RC PagedFileManager::destroyFile(const char *fileName)
{
	if(!FileExists(fileName)) return -1;

	int rc = remove(fileName);
	assert(rc ==0);

	rc = remove(string(fileName).append(INFO).c_str());
	assert(rc ==0);

	return 0;
}


RC PagedFileManager::openFile(const char *fileName, FileHandle &fileHandle)
{
	if(!FileExists(fileName))return -1;
	FILE *pFile = fopen(fileName, "r+b");
	if(pFile!=NULL){
		int rc = fileHandle.setHandle(pFile);
		assert(rc == 0);
	}

	FILE *pInfo = fopen(string(fileName).append(INFO).c_str(), "r+b");
	if(pInfo!=NULL){
		int rc = fileHandle.setPageInfo(pInfo);
		assert(rc == 0);
	}

	int rc = fileHandle.readPageInfo();
	assert(rc == 0);
	return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	int rc = fileHandle.savePageInfo();
	assert(rc == 0);

	return fileHandle.closeHandle();
}


FileHandle::FileHandle()
{
	pFile = NULL;
	pageMaxNum = -1;
	//curr = NULL;
}

/*FileHandle::FileHandle(const char *fileName)
{

}*/



FileHandle::~FileHandle()
{
	/*if(pFile!=NULL){
		fclose(pFile);
	}*/
	pageMaxNum = -1;

//	if(curr != NULL){
//		delete curr;
//	}

	if(fbList.size() != 0){
		fbList.clear();
	}
}

RC FileHandle::closeHandle()
{
	fclose(pageInfo);
	fclose(pFile);
	return 0;
}

RC FileHandle::setHandle(FILE *p)
{
	pFile = p;
	return 0;
}

RC FileHandle::setPageInfo(FILE *p)
{
	pageInfo = p;
	return 0;
}

RC FileHandle::readPageInfo()
{
//	printf("......entering readPageInfo......\n");
	int offset = 0;
	fseek(pageInfo, offset, SEEK_SET);
	fread(&pageMaxNum, sizeof(unsigned), 1, pageInfo);
	offset += sizeof(unsigned);
	//printf("pageMaxNum: %d\n", pageMaxNum);

	fbList.clear();
	pageSlotDirectory.clear();
	for (int i = 0; i < pageMaxNum + 1; i++) {
		FreeBytes *fb = (FreeBytes *)malloc(sizeof(FreeBytes));//fbList[i];
		fseek(pageInfo, offset, SEEK_SET);
		fread(fb, sizeof(FreeBytes), 1, pageInfo);
		fbList.push_back(fb);
		offset += sizeof(FreeBytes);
		//printf(" %d, page: %d free bytes: %d\n", i, fb->getPageNum(), fb->getFreeBytes());
	}

	for (int i = 0; i < pageMaxNum + 1; i++) {
		SlotDirectory *sd = new SlotDirectory();//pageSlotDirectory[i];

		fseek(pageInfo, offset, SEEK_SET);
		int pageNum; //= sd->getPageNum();
		fread(&pageNum, sizeof(int), 1, pageInfo);
		sd->setPageNum(pageNum);
		offset += sizeof(int);
		//printf("page number: %d \t", pageNum);

		fseek(pageInfo, offset, SEEK_SET);
		int lastSlotNum; //= sd->getLastSlotNum();
		fread(&lastSlotNum, sizeof(int), 1, pageInfo);
		sd->setLastSlotNum(lastSlotNum);
		offset += sizeof(int);
		//printf("last slot number: %d\n", lastSlotNum);

		for (int j = 0; j < lastSlotNum + 1; j++)
		{
			SlotDirectoryNode *p = (SlotDirectoryNode *)malloc(sizeof(SlotDirectoryNode));
			fseek(pageInfo, offset, SEEK_SET);
			//SlotDirectoryNode *p = sd->getSlotNode(j);
			fread(p, sizeof(SlotDirectoryNode), 1, pageInfo);
			sd->appendSlotNode(p);
			offset += sizeof(SlotDirectoryNode);
			//printf("slot: %d, offset: %d, length:%d \n", j, p->getSlotOffset(), p->getSlotLength());
		}

		pageSlotDirectory.push_back(sd);

	}
	return 0;
}

RC FileHandle::savePageInfo()
{
//	printf("......entering savePageInfo......\n");
	int offset = 0;
	fseek(pageInfo, offset, SEEK_SET);
	fwrite(&pageMaxNum, sizeof(unsigned), 1, pageInfo);
	offset += sizeof(unsigned);
//	printf("pageMaxNum: %d\n", pageMaxNum);

	for(int i=0; i<pageMaxNum+1; i++)
	{
		FreeBytes *fb = fbList[i];
		fseek(pageInfo, offset, SEEK_SET);
		fwrite(fb, sizeof(FreeBytes), 1, pageInfo);
		offset += sizeof(FreeBytes);
//		printf(" %d, page: %d free bytes: %d\n", i, fb->getPageNum(), fb->getFreeBytes());
	}

	for(int i=0; i<pageMaxNum+1; i++)
	{
		SlotDirectory *sd = pageSlotDirectory[i];
		fseek(pageInfo, offset, SEEK_SET);
		unsigned pageNum = sd->getPageNum();
		fwrite(&pageNum, sizeof(unsigned), 1, pageInfo);
		offset += sizeof(unsigned);
//		printf("page number: %d \t", pageNum);

		fseek(pageInfo, offset, SEEK_SET);
		unsigned lastSlotNum = sd->getLastSlotNum();
		fwrite(&lastSlotNum, sizeof(unsigned), 1, pageInfo);
		offset += sizeof(unsigned);
//		printf("last slot number: %d\n", lastSlotNum);

		for(int j=0;j<sd->getLastSlotNum()+1;j++)
		{
			fseek(pageInfo, offset, SEEK_SET);
			SlotDirectoryNode *p = sd->getSlotNode(j);
			fwrite(p, sizeof(SlotDirectoryNode), 1, pageInfo);
			offset += sizeof(SlotDirectoryNode);

//			printf("slot: %d, offset: %d\n", j, p->getSlotOffset());

		}

	}
	return 0;

}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
	printf("......entering readPage()......\n");
	if(pageMaxNum == -1){
		printf("pageMaxNum = -1\n");
		return -1001;
	}
	if(pageMaxNum < pageNum)
	{
		printf("pageMaxNum < pageNum\n");
		printf("pagenumber: %u, maxPageNUm : %i\n",pageNum, pageMaxNum);
		return -1000;
	}else{
		printf("pagenumber: %u, maxPageNUm : %i\n",pageNum, pageMaxNum);
	}
	//memset(data, '\0', PAGE_SIZE);

	fseek (pFile, pageNum * PAGE_SIZE, SEEK_SET);
	//data = (char*) malloc (sizeof(char)*PAGE_SIZE);

	int count = fread(data, 1, PAGE_SIZE, pFile);

	printf("pFile :%ld, count %d \n", pFile, count);

	//rewind(pFile);
    return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	if(pageMaxNum == -1){
		return -1001;
	}
	if(pageMaxNum < pageNum)
	{
		return -1000;
	}else{
		printf("pagenumber: %u, maxPageNUm : %i\n",pageNum, pageMaxNum);
	}
	printf("writePage: data to be written %s\n", data);
	fseek (pFile, pageNum * PAGE_SIZE, SEEK_SET);
	int count = fwrite(data, 1, PAGE_SIZE, pFile);

	printf("writePage:%ld, %d, %s\n", pFile, count, data);
	//fseek(pFile, 0, SEEK_SET);
	rewind(pFile);
    return 0;
}


RC FileHandle::appendPage(const void *data)
{
	printf("......entering appendPage().......\n");
	pageMaxNum++;
	fseek(pFile, pageMaxNum * PAGE_SIZE, SEEK_SET);
	int count = fwrite(data, 1, PAGE_SIZE, pFile);
	printf("new page appended! no.%d, bytes: %d \n", pageMaxNum, count);

	int rc = appendToPageFreeSpace();
	assert(rc == 0);

	rc = appendToSlotDirectory();
	assert(rc == 0);

    return 0;
}


unsigned FileHandle::getNumberOfPages()
{
    return pageMaxNum+1;
}

RC FileHandle::reduceFreeBytes(unsigned pageNum, int bytes)
{
	printf("......entering reduceFreeBytes()......\n");
	if(pageNum > pageMaxNum)
		return -15;
	FreeBytes *fb = fbList[pageNum];
	/***for test **/
	printf("reduce %d bytes in page %d\n", bytes, pageNum);
	int rc = fb->reduceFreeBytes(bytes);
	assert(rc == 0);
	return 0;
}


RC FileHandle::addFreeBytes(unsigned pageNum, int bytes)
{
	if(pageNum > pageMaxNum)
			return -15;
	FreeBytes *fb = fbList[pageNum];
	return fb->addFreeBytes(bytes);
}


const int FileHandle::getFreeBytes(unsigned pageNum)
{
	if(pageNum > pageMaxNum)
			return -15;
	FreeBytes *fb = fbList[pageNum];
	return fb->getFreeBytes();
}


RC FileHandle::reset(unsigned pageNum)
{
	if(pageNum > pageMaxNum)
			return -15;
	FreeBytes *fb = fbList[pageNum];
	return fb->reset();
}


RC FileHandle::appendToPageFreeSpace()
{
	printf("......entering appendToPageFreeSpace()......\n");
	FreeBytes *fb = new FreeBytes();

	fb->setFreeBytes(PAGE_SIZE);
	fb->setPageNum(pageMaxNum);

	fbList.push_back(fb);

//	if(curr != NULL){
//		curr->next =fb;
//
//	}
//	curr = fb;
	printf("append free space for page. %d, bytes: %d \n", pageMaxNum, fb->getFreeBytes());
	return 0;
}


RC FileHandle::appendToSlotDirectory()
{
	printf("......entering appendToSlotDirectory()......\n");
	SlotDirectory *sdptr = new SlotDirectory();
	sdptr->setPageNum(pageMaxNum);

	pageSlotDirectory.push_back(sdptr);

	printf("append slot directory for page. %d\n", pageMaxNum);
	return 0;
}

//do free space management before append slotdir(offset, bytes) to the slotdirectory of pageNum
RC FileHandle::appendSlot(unsigned pageNum, int bytes)
{
	printf("......entering appendSlot()......\n");
	if(pageNum > pageMaxNum) return -21;
	//reduce the free bytes of page: pageNum
	/*FreeBytes * fb= fbList[pageNum];
	if(fb->getFreeBytes()< bytes){
		return -25;
	}
	int rc = fb->reduceFreeBytes(bytes);
	printf("appendSlot, error code: %d", rc);
	if(rc != 0) return rc;
*/
	//insert the slot directory
	SlotDirectory *p = pageSlotDirectory[pageNum];
	int offset = p->getEndPosition();
	printf("new slot offset is %d\n", offset);

	//pageNum append slot(offset, bytes)
	int rc = p->appendSlot(offset, bytes);
	printf("append a new slot no.%d for page. %d, offset: %d\n", p->getLastSlotNum(), pageMaxNum, offset);
	return rc;
}
/*
RC FileHandle::reorganizePage(int pageNum)
{
	//read page

	//re-organize

	//write back

}*/


unsigned FileHandle::lookforSlot(unsigned pNum, int bytesNeed)
{
	if(pNum>pageMaxNum) return -21;
	SlotDirectory * p = pageSlotDirectory[pNum];
	return p->lookforSlotNum(bytesNeed);

}

unsigned FileHandle::lookforPage(int bytesNeed){
	FreeBytes *fb;
	for(int i=0; i< fbList.size(); i++)
	{
		fb = fbList[i];
		if(fb->getFreeBytes() >= bytesNeed){
			printf("lookforPage: available page found, No. %d\n", i);
			return i;
		}
	}
	printf("no available page found\n");
	return -1;
}

RC FileHandle::updateSlotLen(unsigned pageNum, int slotNum, int len)
{
	if(pageNum > pageMaxNum)return -21;
	SlotDirectory * p = pageSlotDirectory[pageNum];
	int rc = p->updateSlotLength(slotNum, len);
	return rc;
}

RC FileHandle::updateSlotOffset(unsigned pageNum, int slotNum, int off)
{
	if(pageNum > pageMaxNum)return -21;
	SlotDirectory * p = pageSlotDirectory[pageNum];
	int rc = p->updateSlotOffset(slotNum, off);
	return rc;
}

int FileHandle::getOffset(unsigned pageNum, int slotNum)
{
	if(pageNum > pageMaxNum)
	{
		printf("pageNum:%d, pageMaxNum:%d\n",pageNum, pageMaxNum);
		return -21;
	}
	SlotDirectory *p = pageSlotDirectory[pageNum];
	int offset = p->getOffset(slotNum);
	printf("offset in page.%d for slot.%d: %d\n", pageNum, slotNum, offset);
	return offset;
}

int FileHandle::getLength(unsigned pageNum, int slotNum)
{
	if(pageNum > pageMaxNum)
	{
		printf("pageNum:%d, pageMaxNum:%d\n",pageNum, pageMaxNum);
		return -21;
	}
	SlotDirectory *p = pageSlotDirectory[pageNum];
	int len = p->getLength(slotNum);
	printf("byte size in page.%d for slot.%d: %d\n", pageNum, slotNum, len);
	return len;
}

int FileHandle::getEndPosition(unsigned PageNum){
	if(PageNum > pageMaxNum) return -21;
	SlotDirectory * p = pageSlotDirectory[PageNum];
	return p->getEndPosition();
}

const int FileHandle::getLastSlotNum(unsigned PageNum){
	if(PageNum > pageMaxNum) return -21;
	SlotDirectory * p = pageSlotDirectory[PageNum];
	return p->getLastSlotNum();
}


RC FileHandle::setSlotDeleted(unsigned pageNum, int slotNum)
{
	if(pageNum > pageMaxNum) return -21;
	SlotDirectory * p = pageSlotDirectory[pageNum];
	return p->setSlotDeleted(slotNum);
}

const bool FileHandle::isSlotDeleted(unsigned pageNum, int slotNum)
{
	if(pageNum > pageMaxNum) return -21;
	SlotDirectory * p = pageSlotDirectory[pageNum];
	return p->isSlotDeleted(slotNum);
}

RC FileHandle::setSlotTombStone(unsigned pageNum, int slotNum)
{
	if(pageNum > pageMaxNum) return -21;
	SlotDirectory * p = pageSlotDirectory[pageNum];
	return p->setSlotTombStone(slotNum);
}

const bool FileHandle::isSlotTombStone(unsigned pageNum, int slotNum)
{
	if(pageNum > pageMaxNum) return -21;
	SlotDirectory * p = pageSlotDirectory[pageNum];
	return p->isSlotTombSone(slotNum);
}

RC FileHandle::setSlotTombStoneRID(unsigned pageNum, int slotNum, const RID &rid)
{
	if(pageNum > pageMaxNum) return -21;
	SlotDirectory * p = pageSlotDirectory[pageNum];
	return p->setSlotTombStoneRID(slotNum, rid);
}

RC FileHandle::getSlotTombStoneRID(unsigned pageNum, int slotNum, RID &rid)
{
	if(pageNum > pageMaxNum) return -21;
	SlotDirectory * p = pageSlotDirectory[pageNum];
	return p->getSlotTombStoneRID(slotNum, rid);
}
