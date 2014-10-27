
#include "rbfm.h"
//#include "pfm.h"

#include "iostream"
#include <stdlib.h>
#include <cassert>

typedef enum { TypeInteger = 0, TypeRealNUM, TypeVarCH } AttrType1;


/********/
RBFM_ScanIterator::RBFM_ScanIterator()
{
	pos = -1;
}

RC RBFM_ScanIterator::setAttribtues(const vector<Attribute> recordDsp)
{
	for(int i=0; i< recordDsp.size(); i++)
	{
		recordDescriptor.push_back(recordDsp[i]);
	}
	return 0;
}

RC RBFM_ScanIterator::setAttributeNames(const vector<string> &attriNames)
{
	for(int i=0; i<attriNames.size(); i++)
	{
		attributeNames.push_back(attriNames[i]);

		for(int j=0; j<recordDescriptor.size();j++)
		{
			if(strcmp(attriNames[i].c_str(), recordDescriptor[j].name.c_str()) ==0)
			{
				attributePosition.push_back(j);
			}
		}
	}

	return 0;
}

RC RBFM_ScanIterator::addPosition(const RID &rid)
{
	position.push_back(rid);
	return 0;
}

RC RBFM_ScanIterator::setFileHandle(FileHandle &fileHandle)
{
	fileHandlePtr = &fileHandle;
	return 0;
}

RC RBFM_ScanIterator::setRecordBasedFileManager(RecordBasedFileManager *rbfm)
{
	rbfmPtr = rbfm;
	return 0;
}


RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{
	if (position.size() == 0) {
		return -1005;
	}

	if (pos == position.size()) {
		return RBFM_EOF;
	}

	rid.pageNum = position[pos].pageNum;
	rid.slotNum = position[pos].slotNum;

	int bufferLen = fileHandlePtr->getLength(rid.pageNum, rid.slotNum);
	//getByteSize(recordDescriptor, data);

	void *pagedata = (void *) malloc(bufferLen);

	rbfmPtr->readRecord(*fileHandlePtr, recordDescriptor, rid, pagedata);

//	//read the record data of rid
//	void *buffer = malloc(PAGE_SIZE);
//	printf("\n.....entering getNextRecord(): page.%d, slot.%d\n", rid.pageNum,
//			rid.slotNum);
//
//	int rc = fileHandle.readPage(rid.pageNum, buffer);
//	if (rc != 0) {
//		printf("failed to read page: %d\n", rid.pageNum);
//		return -1001;
//	}
//
//	printf("****** before reading......\n");
//
//	int pageOffset = fileHandle.getOffset(rid.pageNum, rid.slotNum);
//	printf("page offset is %d\n", pageOffset);
//	printRecord(recordDescriptor, (char *) buffer + pageOffset);
//
//	Attribute attr;
//
//	//get the byte size of rid
//
//
//	memcpy(pagedata, (char *) buffer + pageOffset, bufferLen);
//
//	printf("****** after getNextRecord......\n");
//	printRecord(recordDescriptor, pagedata);
//

	//no need to calculate the length of data, because the data must be malloc with enough space

	int num = 0;
	int recordoffset = 0;
	int dataoffset = 0;
	int datalen = 0;

	for (int j = 0; j < recordDescriptor.size(); j++) {
		if (j == attributePosition[num]) {
			num++;
			switch (recordDescriptor[j].type) {
			case TypeInt:
				datalen = 4;
				memcpy((char *) data + dataoffset,
						(char *) pagedata + recordoffset, datalen);
				dataoffset += 4;
				break;
			case TypeReal:
				datalen = 4;
				memcpy((char *) data + dataoffset,
						(char *) pagedata + recordoffset, datalen);
				dataoffset += 4;
				break;
			case TypeVarChar:
				recordoffset += 4;
				datalen = recordDescriptor[j].length;
				memcpy((char *) data + dataoffset,
						(char *) pagedata + recordoffset, datalen);
				dataoffset += 4;
				break;
			default:
				break;
			}
		}

		switch (recordDescriptor[j].type) {
		case TypeInt:
			recordoffset += 4;
			break;
		case TypeReal:
			recordoffset += 4;
			break;
		case TypeVarChar:
			recordoffset += 4;
			recordoffset += recordDescriptor[j].length;
			break;
		default:
			break;
		}
	}

	pos++;
	return 0;
}

RC RBFM_ScanIterator::close()
{
	//iterator is controlled by the "pos"
	pos = -1;

	position.clear();
	recordDescriptor.clear();
	attributePosition.clear();
	attributeNames.clear();
	rbfmPtr = NULL;
	fileHandlePtr = NULL;
	return 0;
}

/*******/


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
			}else{
				//re-organize page, then insert.
				printf("re-organize a page!\n");
				reorganizePage(fileHandle, recordDescriptor, pageNum);
				offset = fileHandle.getEndPosition(pageNum);
				memcpy((char *)buffer + offset, (char *)data,  bytesNeed);
			}
			rc = fileHandle.reduceFreeBytes(pageNum, bytesNeed);
			if (rc != 0)
				return rc;

			rc = fileHandle.appendSlot(pageNum, bytesNeed);
			if (rc != 0)
				return rc;

			rid.slotNum = fileHandle.getLastSlotNum(pageNum);
			//*******modify rid.slotNum
			printf("record appended to the end of the slots in a page!\n");
			printRecord(recordDescriptor, (char *) buffer + offset);

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
		return -1001;
	}

	printf("****** before reading......\n");

	int pageOffset = fileHandle.getOffset(rid.pageNum, rid.slotNum);
	printf("page offset is %d\n", pageOffset);
	printRecord(recordDescriptor, (char *)buffer + pageOffset);

	Attribute attr;

	//get the byte size of rid
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


/************** implementation for the 2nd project  *********/

RC RecordBasedFileManager::deleteRecords(FileHandle &fileHandle)
{
 return 0;
}

// similiar with readRecord, after readrecord, 1. modify the record to "deleted", 2) set each bit of the record to "0"   3) add to the freespace of the page by the record length. 4) write back
RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
	//read record
	void *buffer = malloc(PAGE_SIZE);
	printf("\n.....entering deleteRecord(): page.%d, slot.%d\n", rid.pageNum, rid.slotNum);
	int rc = fileHandle.readPage(rid.pageNum, buffer);
	if(rc != 0){
		printf("failed to read page: %d\n", rid.pageNum);
	}


	//set each bit of the record to "0"
	printf("****** before delete record......\n");
	int pageOffset = fileHandle.getOffset(rid.pageNum, rid.slotNum);
	printf("page offset is %d\n", pageOffset);
	printRecord(recordDescriptor, (char *)buffer + pageOffset);

	Attribute attr;

	//get the byte size of data to be deleted from slot directory
	int dataLen = fileHandle.getLength(rid.pageNum, rid.slotNum);//getByteSize(recordDescriptor, data);

	memset((char *)buffer + pageOffset, 0, dataLen);
	printf("****** after set to 0......\n");

	printRecord(recordDescriptor, (char *)buffer + pageOffset);

	//modify the record to "delete" in the slot directory
	rc = fileHandle.setSlotDeleted(rid.pageNum, rid.slotNum);
	assert(rc == 0);
	//modify the slot length of the deleted record to 0
	rc = fileHandle.updateSlotLen(rid.pageNum, rid.slotNum, 0);
	assert(rc == 0);

	//add to the freespace of the page by the record length.
	rc = fileHandle.addFreeBytes(rid.pageNum, dataLen);
	assert(rc == 0);

	//write back
	rc = fileHandle.writePage(rid.pageNum, buffer);
	assert(rc == 0);

	return 0;
}


RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
	//read record
	void *buffer = malloc(PAGE_SIZE);
	printf("\n.....entering updateRecord(): page.%d, slot.%d\n", rid.pageNum, rid.slotNum);
	int rc = fileHandle.readPage(rid.pageNum, buffer);
	if(rc != 0){
		printf("failed to read page: %d\n", rid.pageNum);
	}

	//modify the record to *data  1.get offset from rid,
	int pageOffset = fileHandle.getOffset(rid.pageNum, rid.slotNum);
	printf("page offset is %d\n", pageOffset);

	//2.get data length from attribute
	int dataLen = getByteSize(recordDescriptor, data);

	//check if there enough space between offset of rid.slotNum and offset of rid.slotNum+1
	// if slotNum is the last slot.
	int freespace = 0;
	if(fileHandle.getLastSlotNum(rid.pageNum) == rid.pageNum){
		 freespace = PAGE_SIZE - pageOffset;
	}else{
		int pageOffset_plus = fileHandle.getOffset(rid.pageNum, rid.slotNum+1);
		freespace = pageOffset_plus - pageOffset;
	}

	if(freespace >= dataLen)
	{
		memcpy((char *)buffer + pageOffset, data, dataLen);
	}else{
		//migrate
		//how to migrate the data?? how to leave new rid??
	}

	//modify the data length in slot directory of rid
	rc = fileHandle.updateSlotLen(rid.pageNum, rid.slotNum, dataLen);
	assert(rc == 0);

	//write back
	rc = fileHandle.writePage(rid.pageNum, buffer);
	assert(rc == 0);

	return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string attributeName, void *data)
{
	int recordlen = fileHandle.getLength(rid.pageNum, rid.slotNum);
	void *recordData = (void *)malloc(recordlen);
	readRecord(fileHandle, recordDescriptor, rid, recordData);

	int offset = 0;
	for(int i=0; i<recordDescriptor.size(); i++)
	{
		//find the attributename in the recordDescriptor
		if(strcmp(attributeName.c_str(), recordDescriptor[i].name.c_str())==0)
		{
			int datalen;
			switch(recordDescriptor[i].type){
					case TypeInt:
						datalen = 4;
						break;
					case TypeReal:
						datalen = 4;
						break;
					case TypeVarChar:
						offset += 4;//namelength
						datalen = recordDescriptor[i].length;
						break;
					default:
						break;
					}
			//data = (void *)malloc(datalen);
			memcpy(data, (char *)recordData + offset, datalen);
		}

		//get the offset from the start of data
		switch (recordDescriptor[i].type) {
		case TypeInt:
			offset += 4;
			break;
		case TypeReal:
			offset += 4;
			break;
		case TypeVarChar:
			offset += 4; //namelength
			offset += recordDescriptor[i].length;
			break;
		default:
			break;
		}
	}

	return 0;
}


RC RecordBasedFileManager::reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNumber)
{
	//read
	void *buffer = malloc(PAGE_SIZE);
	printf("\n.....entering reorganizePage(): page.%d \n", pageNumber);
	int rc = fileHandle.readPage(pageNumber, buffer);
	if (rc != 0) {
		printf("failed to read page: %d\n", pageNumber);
	}

	//re-organize
	int maxSlotNum = fileHandle.getLastSlotNum(pageNumber);
	int shiftSize = 0;
	for(int slotNum = 0; slotNum < maxSlotNum; slotNum++)
	{
		int offset = fileHandle.getOffset(pageNumber, slotNum);
		int len = fileHandle.getLength(pageNumber, slotNum);

		int offset_plus = fileHandle.getOffset(pageNumber, slotNum+1);
		int len_plus = fileHandle.getLength(pageNumber, slotNum+1);

		//modify the shftsize of the following slot
		if((offset_plus - offset) > len){
			shiftSize += offset_plus - offset - len;
		}

		if(shiftSize > 0)
		{
			//modify the slot offset in slot directory
			rc = fileHandle.updateSlotOffset(pageNumber, slotNum+1, offset_plus-shiftSize);

			//move the data in page
			memcpy((char *)buffer + offset_plus -shiftSize, (char *)buffer + offset_plus, len_plus);
		}

	}


	//write back
	rc = fileHandle.writePage(pageNumber, buffer);
	assert(rc == 0);

	return 0;

}


RC RecordBasedFileManager::scan(FileHandle &fileHandle,
    const vector<Attribute> &recordDescriptor,
    const string &conditionAttribute,
    const CompOp compOp,                  // comparision type such as "<" and "="
    const void *value,                    // used in the comparison
    const vector<string> &attributeNames, // a list of projected attributes
    RBFM_ScanIterator &rbfm_ScanIterator)
{
	//no page
	if(fileHandle.getNumberOfPages() == 0){
		return RBFM_EOF;
	}

	rbfm_ScanIterator.setAttribtues(recordDescriptor);
	rbfm_ScanIterator.setAttributeNames(attributeNames);
	rbfm_ScanIterator.setRecordBasedFileManager(this);
	rbfm_ScanIterator.setFileHandle(fileHandle);

	int pageNum = 0;
	int slotNum = 0;

	int type;
	int valuelen = 0;
	for(int i=0; i < recordDescriptor.size(); i++)
	{
		if(strcmp(conditionAttribute.c_str(), recordDescriptor[i].name.c_str())==0)
		{
			switch (recordDescriptor[i].type) {
					case TypeInt:
						type = 0;
						break;
					case TypeReal:
						type = 1;
						break;
					case TypeVarChar:
						type = 2;
						valuelen = recordDescriptor[i].length;
						break;
					default:
						break;
					}

		}
	}

	bool found = false;//if the record satisfies the requirement.

	//retrieve each record in the file
	while(((pageNum +1) <= fileHandle.getNumberOfPages()) && slotNum <= fileHandle.getLastSlotNum(pageNum))
	{
		RID rid;
		rid.pageNum = pageNum;
		rid.slotNum = slotNum;

		//void * conditionAttributeData = (void *)malloc(datalen);
		//readAttribute(fileHandle, recordDescriptor, rid, conditionAttribute, conditionAttributeData);
		//check if data satisfies the requirement
		if (type == 0) {
			int conditionAttributeData;
			readAttribute(fileHandle, recordDescriptor, rid, conditionAttribute,
					&conditionAttributeData);
			printf("conditionAttributeData: %d\n", conditionAttributeData);
			int *attriValue = (int *) value;

			switch (compOp) {
			case EQ_OP:
				if (*attriValue == conditionAttributeData) {
					found = true;
				}
				break;
			case LT_OP:
				if (*attriValue < conditionAttributeData) {
					found = true;
				}
				break;
			case GT_OP:
				if (*attriValue > conditionAttributeData) {
					found = true;
				}
				break;
			case LE_OP:
				if (*attriValue <= conditionAttributeData) {
					found = true;
				}
				break;
			case GE_OP:
				if (*attriValue >= conditionAttributeData) {
					found = true;
				}
				break;
			case NE_OP:
				if (*attriValue != conditionAttributeData) {
					found = true;
				}
				break;
			case NO_OP:
				found = true;
				break;
			}
		} else if (type == 1) {
			float conditionAttributeData;
			readAttribute(fileHandle, recordDescriptor, rid, conditionAttribute,
					&conditionAttributeData);
			printf("conditionAttributeData: %f\n", conditionAttributeData);
			float *attriValue = (float *) value;

			switch (compOp) {
			case EQ_OP:
				if (*attriValue == conditionAttributeData) {
					found = true;
				}
				break;
			case LT_OP:
				if (*attriValue < conditionAttributeData) {
					found = true;
				}
				break;
			case GT_OP:
				if (*attriValue > conditionAttributeData) {
					found = true;
				}
				break;
			case LE_OP:
				if (*attriValue <= conditionAttributeData) {
					found = true;
				}
				break;
			case GE_OP:
				if (*attriValue >= conditionAttributeData) {
					found = true;
				}
				break;
			case NE_OP:
				if (*attriValue != conditionAttributeData) {
					found = true;
				}
				break;
			case NO_OP:
				found = true;
				break;
			}
		} else {
			char *conditionAttributeData = (char *) malloc(valuelen);
			readAttribute(fileHandle, recordDescriptor, rid, conditionAttribute,
					conditionAttributeData);
			printf("conditionAttributeData: %s\n", conditionAttributeData);
			char *attriValue = (char *) value;

			switch (compOp) {
			case EQ_OP:
				if (strcmp(attriValue, conditionAttributeData) == 0) {
					found = true;
				}
				break;
			case LT_OP:
				if (strcmp(attriValue, conditionAttributeData) < 0) {
					found = true;
				}
				break;
			case GT_OP:
				if (strcmp(attriValue, conditionAttributeData) > 0) {
					found = true;
				}
				break;
			case LE_OP:
				if (strcmp(attriValue, conditionAttributeData) <= 0) {
					found = true;
				}
				break;
			case GE_OP:
				if (strcmp(attriValue, conditionAttributeData) >= 0) {
					found = true;
				}
				break;
			case NE_OP:
				if (strcmp(attriValue, conditionAttributeData) != 0) {
					found = true;
				}
				break;
			case NO_OP:
				found = true;
				break;
			}

			free(conditionAttributeData);
		}

		if(found){
			rbfm_ScanIterator.addPosition(rid);
		}

		if(slotNum == fileHandle.getLastSlotNum(pageNum)){
			slotNum = 0;
			pageNum ++;
		}else{
			slotNum ++;
		}
	}

	return 0;
}
