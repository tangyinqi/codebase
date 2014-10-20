
#include "rbfm.h"
//#include "pfm.h"

#include "iostream"
#include <stdlib.h>
#include <cassert>

typedef enum { TypeInteger = 0, TypeRealNUM, TypeVarCH } AttrType1;


RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {

	int rc = PagedFileManager::instance()->createFile(fileName.c_str());
	return rc;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    int rc = PagedFileManager::instance()->destroyFile(fileName.c_str());
	return rc;
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
	int rc = PagedFileManager::instance()->openFile(fileName.c_str(), fileHandle);
    return rc;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
	int rc = PagedFileManager::instance()->closeFile(fileHandle);
    return rc;
}


int RecordBasedFileManager::getByteSize(const vector<Attribute> &recordDescriptor, const void *data)
{
	int offset = 0;
	Attribute attr;
	int namelength;
	for(int i=0; i< recordDescriptor.size(); i++)
	{
		attr = recordDescriptor[i];
		switch(attr.type){
		case 0:
			offset += sizeof(int);
			break;
		case 1:
			offset += sizeof(float);
			break;
		case 2:
			memcpy(&namelength, (char *) data + offset, sizeof(int));
			//namelenght = *(int *)data + offset;
			offset += sizeof(int);
			//read the name//
//			char *name = (char *) malloc(namelength + 1);
//			memset(name, '\0', namelenght + 1);
//			memcpy(name, (char *) data + offset, namelenght);
//			cout << recordDescriptor[i].name << ": " << name << "\t";
			offset += namelength;
			break;
		default:
			break;
		}
		//bytesNeed += attr.length;
	}
	printf("the bytesNeed is: %d\n", offset);
	return offset;
}


RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
	//get the byte size of all attribute
	int rc = 0;
	/*****   test  *****/
	printf("insertRecord: \n");
	rc = printRecord(recordDescriptor, data);
	if(rc != 0)return rc;

	int bytesNeed = getByteSize(recordDescriptor, data);

	int pageNum = fileHandle.lookforPage(bytesNeed);
	void *buffer = (void *)malloc(PAGE_SIZE);

	if(pageNum==-1){

		memset(buffer, 0, PAGE_SIZE);
		memcpy((char *)buffer, data, bytesNeed);

		printRecord(recordDescriptor, buffer);
		rc = fileHandle.appendPage(buffer);
		assert(rc == 0);

//		rc = fileHandle.appendToPageFreeSpace();
//		assert(rc == 0);
//
//		rc = fileHandle.appendToSlotDirectory();
//		assert(rc == 0);

		rc = fileHandle.reduceFreeBytes(fileHandle.getNumberOfPages()-1, bytesNeed);
		assert(rc == 0);

		rc = fileHandle.appendSlot(fileHandle.getNumberOfPages()-1, bytesNeed);
		assert(rc == 0);

		rid.pageNum = fileHandle.getNumberOfPages()-1;
		rid.slotNum = 0;

		printf("appendPage, then insert record!\n");



	}else{
		rid.pageNum = pageNum;
		rc = fileHandle.readPage(pageNum,  buffer);
		if(rc != 0)return rc;

		int slotNum = fileHandle.lookforSlot(pageNum, bytesNeed);

		if(slotNum ==-1){

			int offset = fileHandle.getEndPosition(pageNum);
			int freeBytes = PAGE_SIZE - offset;
			if(freeBytes >= bytesNeed){
				//append to page
				memcpy((char *)buffer + offset, (char *)data,  bytesNeed);
				rc = fileHandle.reduceFreeBytes(pageNum, bytesNeed);
				if(rc != 0)return rc;

				rc = fileHandle.appendSlot(pageNum, bytesNeed);
				if(rc != 0)return rc;

				rid.slotNum = fileHandle.getLastSlotNum(pageNum);
				//*******modify rid.slotNum
				printf("record appended to the end of the slots in a page!\n");
				printRecord(recordDescriptor, (char *)buffer + offset);

			}else{
				//re-organize page, then insert.
				printf("waiting to be implemented! will re-organize the file, then append!");
			}

		}else{
			//insert into the slot
			rid.slotNum = slotNum;

			rc = fileHandle.reduceFreeBytes(pageNum, bytesNeed);
			if(rc!=0)return rc;
			int offset = fileHandle.getOffset(pageNum, slotNum);
			memcpy((char *)buffer + offset, (char *)data,  bytesNeed);
			/**modify the slotDir to (offset, bytesNeed), for page PageNum**/
			rc = fileHandle.updateSlotLen(pageNum,slotNum, offset);
			if(rc != 0)return rc;
		}
		printf("write back to page \n\n");
		rc = fileHandle.writePage(pageNum, buffer);

		if(rc != 0)return rc;

	}

	//printRecord(recordDescriptor, buffer);
	return 0;
}



RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	void *buffer = malloc(PAGE_SIZE);
	printf("\n.....entering readRecord(): page.%d, slot.%d\n", rid.pageNum, rid.slotNum);
	int rc = fileHandle.readPage(rid.pageNum, buffer);
	if(rc != 0){
		printf("failed to read page: %d\n", rid.pageNum);
	}

	printf("****** before reading......\n");

	int pageOffset = fileHandle.getOffset(rid.pageNum, rid.slotNum);
	printf("page offset is %d\n", pageOffset);
	printRecord(recordDescriptor, (char *)buffer + pageOffset);

	Attribute attr;

	//get the byte size of all attribute
	int bufferLen = fileHandle.getLength(rid.pageNum, rid.slotNum);//getByteSize(recordDescriptor, data);


	memcpy(data, (char *)buffer + pageOffset, bufferLen);
	printf("****** after memcpying......\n");
	printRecord(recordDescriptor, data);

	/*int offset = 0;
	//int dataoffset=0;
	int intval;
	float floatval;
	unsigned int namelenght;
	for(int i=0; i< recordDescriptor.size(); i++)
	{

		//memcpy((char *)data + offset, (char *)buffer + pageOffset, attr.length);
		switch (recordDescriptor[i].type) {
				case 0:
					memcpy(&intval, (char *) buffer + pageOffset + offset, sizeof(int));
					cout << recordDescriptor[i].name << ": " << intval << "\t";
					memcpy((int *)data+offset, &intval, sizeof(int));
					offset += sizeof(int);
					//dataoffset += sizeof(int);
					break;
				case 1:
					memcpy(&floatval, (char *) buffer + pageOffset + offset, sizeof(float));
					cout << recordDescriptor[i].name << ": " << floatval << "\t";
					memcpy((float *)data+offset, &floatval, sizeof(float));
					offset += sizeof(float);
					//dataoffset += sizeof(float);
					break;
				case 2: //string case
					//read name lengh//
					memcpy(&namelenght, (char *) buffer + pageOffset + offset, sizeof(int));
					//namelenght = *(int *)data + offset;
					offset += sizeof(int);
					//read the name//
					char *name = (char *) malloc(namelenght + 1);
					memset(name, '\0', namelenght + 1);
					memcpy(name, (char *) buffer + pageOffset + offset, namelenght);
					cout << recordDescriptor[i].name << ": " << name << "\t";

					memcpy((char *)data+offset, name, namelenght);
					offset += namelenght;
					//dataoffset += namelenght;
					break;
				}
	}

	*/
	// testing
	printf("\n*********  after reading: ******\n");
	printRecord(recordDescriptor, data);

	return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {


	Attribute attr;
	unsigned int offset = 0;
	unsigned int namelenght, nrofelement, i;
	int intval;
	float floatval;
	nrofelement = (unsigned int) recordDescriptor.size();

	//------------------------//
	for (i = 0; i < nrofelement; i++) {
		switch (recordDescriptor[i].type) {
		case 0:
			memcpy(&intval, (char *) data + offset, sizeof(int));
			cout << recordDescriptor[i].name << ": " << intval << "\t";
			offset += sizeof(int);
			break;
		case 1:
			memcpy(&floatval, (char *) data + offset, sizeof(float));
			cout << recordDescriptor[i].name << ": " << floatval << "\t";
			offset += sizeof(float);
			break;
		case 2: //string case
			//read name lengh//
			memcpy(&namelenght, (char *) data + offset, sizeof(int));
			//namelenght = *(int *)data + offset;
			offset += sizeof(int);
			//read the name//
			char *name = (char *) malloc(namelenght + 1);
			memset(name, '\0', namelenght + 1);
			memcpy(name, (char *) data + offset, namelenght);
			cout << recordDescriptor[i].name << ": " << name << "\t";
			offset += namelenght;
			break;
		}
	}
	cout<<endl;

	return 0;




}
