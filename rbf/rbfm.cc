
#include "rbfm.h"
//#include "pfm.h"



typedef enum { TypeInteger = 0, TypeRealNUM, TypeVarCH } AttrType1;


/********/
RBFM_ScanIterator::RBFM_ScanIterator()
{
	pos = 0;
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
	fileHandlePtr = fileHandle;
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
		printf("### getNextRecord() size=0\n");
		return -1005;
	}

	if (pos == position.size()) {
		printf("### end of file\n");
		return RBFM_EOF;
	}

	rid.pageNum = position[pos].pageNum;
	rid.slotNum = position[pos].slotNum;

	printf("### RBFM_ScanIterator::getNextRecord: no.%d pageNum: %d, slotNum: %d\n", pos, rid.pageNum, rid.slotNum);


	int bufferLen = fileHandlePtr.getLength(rid.pageNum, rid.slotNum);
	//getByteSize(recordDescriptor, data);
	printf("data len: %d", bufferLen);

	void *recorddata = (void *) malloc(bufferLen);

	int rc = rbfmPtr->readRecord(fileHandlePtr, recordDescriptor, rid, recorddata);
	assert(rc==0);

	//no need to calculate the length of data, because the data must be malloc with enough space

	int num = 0;
	int recordoffset = 0;
	int dataoffset = 0;
	int datalen = 0;
	int namelength;
	for (int j = 0; j < recordDescriptor.size(); j++) {
		if (j == attributePosition[num]) {
			num++;
			switch (recordDescriptor[j].type) {
			case TypeInt:
				//datalen = 4;
				memcpy((char *) data + dataoffset,(char *) recorddata + recordoffset, 4);
				dataoffset += 4;
				recordoffset +=4;
				break;
			case TypeReal:
				//datalen = 4;
				memcpy((char *) data + dataoffset, (char *) recorddata + recordoffset, 4);
				dataoffset += 4;
				recordoffset +=4;
				break;
			case TypeVarChar:
				memcpy(&namelength, (char *)recorddata + recordoffset, 4);
				recordoffset += 4;

				//datalen = recordDescriptor[j].length;
				memcpy((char *) data + dataoffset, (char *) recorddata + recordoffset, namelength);
				recordoffset += namelength;
				break;
			default:
				break;
			}

		}else{
			switch (recordDescriptor[j].type) {
			case TypeInt:
				recordoffset += 4;
				break;
			case TypeReal:
				recordoffset += 4;
				break;
			case TypeVarChar:
				memcpy(&namelength, (char *)recorddata + recordoffset, 4);
				recordoffset += 4;
				recordoffset += namelength;
				break;
			default:
				break;
			}
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
	fileHandlePtr.closeHandle();
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
//	printf("the bytesNeed is: %d\n", offset);
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
	memset(buffer, 0, PAGE_SIZE);

	if(pageNum==-1){
		//append to a new page
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
		int pNum = fileHandle.getNumberOfPages()-1;

//		rc = fileHandle.reset(pNum);
//		assert(rc==0);

		unsigned x = fileHandle.getFreeBytes(pNum);
		printf("left FreeByes in %d is %d\n", pNum, x);

		rc = fileHandle.reduceFreeBytes(fileHandle.getNumberOfPages()-1, bytesNeed);
		assert(rc == 0);

		rc = fileHandle.appendSlot(fileHandle.getNumberOfPages()-1, bytesNeed);
		assert(rc == 0);

		rid.pageNum = fileHandle.getNumberOfPages()-1;
		rid.slotNum = 0;

		printf("appendPage, record inserted!\n");

	}else{
		//append to a found page
		rid.pageNum = pageNum;
		rc = fileHandle.readPage(pageNum,  buffer);
		if(rc != 0)return rc;

		//int slotNum = fileHandle.lookforSlot(pageNum, bytesNeed);

		//if(slotNum ==-1){
			//append to the last slot of this page
			int offset = fileHandle.getEndPosition(pageNum);
			//int freeBytes = PAGE_SIZE - offset;
			//if(freeBytes >= bytesNeed){
				//append to page
				memcpy((char *)buffer + offset, (char *)data,  bytesNeed);
		/*	}else{
				//re-organize page, then insert.
				printf("re-organize a page!\n");
				reorganizePage(fileHandle, recordDescriptor, pageNum);
				offset = fileHandle.getEndPosition(pageNum);
				memcpy((char *)buffer + offset, (char *)data,  bytesNeed);
			}*/
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

		/*}else{
			//insert into the slot
			rid.slotNum = slotNum;
			//??????
			int slotLen = fileHandle.getLength(pageNum, slotNum);
			assert(slotLen>=bytesNeed);

			rc = fileHandle.reduceFreeBytes(pageNum, (slotLen-bytesNeed));
			assert(rc==0);

			int offset = fileHandle.getOffset(pageNum, slotNum);
			memcpy((char *)buffer + offset, (char *)data,  bytesNeed);

			//modify the slotDir to (offset, bytesNeed), for page PageNum
			rc = fileHandle.updateSlotLen(pageNum,slotNum, bytesNeed);
			if(rc != 0)return rc;
		}*/

		printf("write back to page \n\n");
		rc = fileHandle.writePage(pageNum, buffer);

		if(rc != 0)return rc;

	}

	//printRecord(recordDescriptor, buffer);
	return 0;
}



RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	if(fileHandle.isSlotDeleted(rid.pageNum, rid.slotNum)){
		printf("####SLOT DELETED!###\n");
		return -2000;
	}
	if((rid.pageNum+1>fileHandle.getNumberOfPages()) && (rid.slotNum > fileHandle.getLastSlotNum(rid.pageNum))){
		printf("####rid exceed the bound!");
		return -1;
	}
	int pNum = rid.pageNum;
	int sNum = rid.slotNum;

	while(fileHandle.isSlotTombStone(pNum, sNum))
	{
		RID ridNext;
		int rc = fileHandle.getSlotTombStoneRID(pNum, sNum, ridNext);
		assert(rc==0);
		pNum = ridNext.pageNum;
		sNum = ridNext.slotNum;
		printf("tombStone: ridNext:%d, %d", pNum, sNum);
	}


	void *buffer = malloc(PAGE_SIZE);
	printf("\n.....entering readRecord(): page.%d, slot.%d\n", pNum, sNum);
	int rc = fileHandle.readPage(pNum, buffer);
	if(rc != 0){
		printf("failed to read page: %d\n", pNum);
		return -1001;
	}

//	printf("****** before reading......\n");

	int pageOffset = fileHandle.getOffset(pNum, sNum);
/*	printf("page offset is %d\n", pageOffset);
	if(recordDescriptor.size() != 0){
		printRecord(recordDescriptor, (char *)buffer + pageOffset);
	}
*/

	Attribute attr;

	//get the byte size of rid
	int bufferLen = fileHandle.getLength(pNum, sNum);//getByteSize(recordDescriptor, data);


	memcpy(data, (char *)buffer + pageOffset, bufferLen);
/*	printf("****** after memcpying......\n");
	if(recordDescriptor.size() != 0){
		printRecord(recordDescriptor, data);
	}

	// testing
	printf("\n*********  after reading: ******\n");
	if(recordDescriptor.size() != 0){
		printRecord(recordDescriptor, data);
	}
*/
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
	int rc;
	for(int pageNum=0; pageNum<fileHandle.getNumberOfPages();pageNum++)
	{
		for(int slotNum=0;slotNum<=fileHandle.getLastSlotNum(pageNum); slotNum++)
		{
			//modify the record to "delete" in the slot directory
			rc = fileHandle.setSlotDeleted(pageNum, slotNum);
			assert(rc == 0);
			//modify the slot length of the deleted record to 0
			//rc = fileHandle.updateSlotLen(pageNum, slotNum, 0);
			//assert(rc == 0);
		}
		void *buffer = malloc(PAGE_SIZE);
		int rc = fileHandle.readPage(pageNum, buffer);
		if(rc != 0){
			printf("failed to read page: %d\n", pageNum);
		}
		//set the page to 0
		memset((char *)buffer, 0, PAGE_SIZE);

		//add to the freespace of the page by the record length.
		rc = fileHandle.reset(pageNum);
		assert(rc == 0);
		//write back

		rc = fileHandle.savePageInfo();
		assert(rc == 0);

		rc = fileHandle.writePage(pageNum, buffer);
		assert(rc == 0);
		free(buffer);
	}
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
	int dataLen = fileHandle.getLength(rid.pageNum, rid.slotNum);
	//getByteSize(recordDescriptor, data);

	memset((char *)buffer + pageOffset, 0, dataLen);
	printf("****** after set to 0......\n");

	printRecord(recordDescriptor, (char *)buffer + pageOffset);

	//modify the record to "delete" in the slot directory, leave the rest unchanged
	rc = fileHandle.setSlotDeleted(rid.pageNum, rid.slotNum);
	assert(rc == 0);
	//modify the slot length of the deleted record to 0
	//rc = fileHandle.updateSlotLen(rid.pageNum, rid.slotNum, 0);
	//assert(rc == 0);

	//add to the freespace of the page by the record length.
	rc = fileHandle.addFreeBytes(rid.pageNum, dataLen);
	assert(rc == 0);

	//write back
	rc = fileHandle.writePage(rid.pageNum, buffer);
	assert(rc == 0);
	free(buffer);
	return 0;
}


RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
	if(fileHandle.isSlotDeleted(rid.pageNum, rid.slotNum))
		return -1;

	int pageNum = rid.pageNum;
	int slotNum = rid.slotNum;
	int rc;
	while(fileHandle.isSlotTombStone(pageNum, slotNum))
	{
		RID ridnext;
		rc = fileHandle.getSlotTombStoneRID(pageNum, slotNum, ridnext);
		pageNum = ridnext.pageNum;
		slotNum = ridnext.slotNum;
	}
	//read record
	void *buffer = malloc(PAGE_SIZE);
	printf("\n####.....entering updateRecord(): page.%d, slot.%d\n", pageNum, slotNum);
	rc = fileHandle.readPage(pageNum, buffer);
	if(rc != 0){
		printf("failed to read page: %d\n", pageNum);
	}

	//modify the record to *data  1.get offset from rid,
	int pageOffset = fileHandle.getOffset(pageNum, slotNum);
	printf("page offset is %d\n", pageOffset);

	//2.get data length from attribute
	int dataLen = getByteSize(recordDescriptor, data);
	int dataLenOld = fileHandle.getLength(pageNum, slotNum);

	if(dataLenOld >= dataLen){
		//insert into the original slot
		memcpy((char *)buffer + pageOffset, data, dataLen);
		rc = fileHandle.updateSlotLen(pageNum, slotNum, dataLen);
		assert(rc == 0);

		if(dataLenOld != dataLen){
			rc = fileHandle.addFreeBytes(pageNum, (dataLenOld-dataLen));
			assert(rc == 0);
		}
		//write back
		rc = fileHandle.savePageInfo();
		assert(rc ==0);
		rc = fileHandle.writePage(pageNum, buffer);
		assert(rc == 0);
		return 0;
	}


	int spacePageEnd = PAGE_SIZE - fileHandle.getEndPosition(pageNum);
	int oldSlotSpace;
	int freebytes = fileHandle.getFreeBytes(pageNum);

	if(spacePageEnd >= dataLen){
		//update to the end of a page
		pageOffset = fileHandle.getEndPosition(pageNum);
		memcpy((char *) buffer + pageOffset, data, dataLen);
		rc = fileHandle.appendSlot(pageNum, dataLen);
		assert(rc == 0);

		rc = fileHandle.reduceFreeBytes(pageNum, dataLen);
		assert(rc == 0);

		rc = fileHandle.setSlotTombStone(pageNum, slotNum);
		assert(rc == 0);
		RID tombStoneRID;
		tombStoneRID.pageNum = pageNum;
		tombStoneRID.slotNum = fileHandle.getLastSlotNum(pageNum);

		rc = fileHandle.setSlotTombStoneRID(pageNum, slotNum, tombStoneRID);
		assert(rc == 0);
	}else{
		//have to be migrated to a different page!
		//migrate: insert the record into a slot with available space size,
		RID tombStoneRID;
		rc = insertRecord(fileHandle, recordDescriptor, data, tombStoneRID);
		assert(rc == 0);
		//set new rid
		rc = fileHandle.setSlotTombStone(pageNum, slotNum);
		assert(rc == 0);

		rc = fileHandle.setSlotTombStoneRID(pageNum, slotNum, tombStoneRID);
		assert(rc == 0);

	}
	/*
	if(freebytes >= dataLen){
		//can insert into the same page
		if(fileHandle.getLastSlotNum(pageNum) == slotNum){
			oldSlotSpace = PAGE_SIZE - pageOffset;
			 printf("last slot, spaceOfPageEnd = %d \n", oldSlotSpace);
		}else{
			int pageOffset_plus = fileHandle.getOffset(pageNum, slotNum+1);
			oldSlotSpace = pageOffset_plus - pageOffset;
			printf("oldSlotSpace=%d , dataLen=%d\n", oldSlotSpace, dataLen);
		}

		if(oldSlotSpace >= dataLen){
			//the original slot has available space
			memcpy((char *)buffer + pageOffset, data, dataLen);

			rc = fileHandle.updateSlotLen(pageNum, slotNum, dataLen);
			assert(rc == 0);
			assert(dataLenOld < dataLen);

			rc = fileHandle.reduceFreeBytes(pageNum, (dataLen-dataLenOld));
			assert(rc == 0);
		}else{
			//the end of the page
			pageOffset = fileHandle.getEndPosition(pageNum);
			spacePageEnd = PAGE_SIZE-pageOffset;
			if(spacePageEnd < dataLen){
				//need to re-organize
				printf("####### spacePageEnd=%d, dataLen=%d. need to reorganize Page", spacePageEnd, dataLen);
				rc = reorganizePage(fileHandle, recordDescriptor, pageNum);
				assert(rc == 0);

				pageOffset = fileHandle.getEndPosition(pageNum);
				spacePageEnd = PAGE_SIZE-spacePageEnd;
				//check if there is enough space in page this time
				assert(spacePageEnd >= dataLen);
			}

			memcpy((char *)buffer + pageOffset, data, dataLen);

			//append a new slot to the end of page
			rc = fileHandle.appendSlot(pageNum, dataLen);
			assert(rc == 0);

			rc = fileHandle.reduceFreeBytes(pageNum, dataLen);
			assert(rc == 0);

			rc = fileHandle.setSlotTombStone(pageNum, slotNum);
			assert(rc == 0);
			RID tombStoneRID;
			tombStoneRID.pageNum = pageNum;
			tombStoneRID.slotNum = fileHandle.getLastSlotNum(pageNum);

			rc = fileHandle.setSlotTombStoneRID(pageNum, slotNum, tombStoneRID);
			assert(rc == 0);
		}
		//write back
//		rc = fileHandle.writePage(pageNum, buffer);
//		assert(rc == 0);

	}else{
		//have to be migrated to a different page!
		//migrate: insert the record into a slot with available space size,
		RID tombStoneRID;
		rc = insertRecord(fileHandle, recordDescriptor, data, tombStoneRID);
		assert(rc == 0);
		//set new rid
		rc = fileHandle.setSlotTombStone(pageNum, slotNum);
		assert(rc == 0);

		rc = fileHandle.setSlotTombStoneRID(pageNum, slotNum, tombStoneRID);
		assert(rc == 0);
	}

*/


	//modify the data length in slot directory of rid
//	rc = fileHandle.updateSlotLen(rid.pageNum, rid.slotNum, dataLen);
//	assert(rc == 0);

	//write back
	rc = fileHandle.savePageInfo();
	assert(rc ==0);

	rc = fileHandle.writePage(pageNum, buffer);
	assert(rc == 0);

	return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string attributeName, void *data)
{
	int recordlen = fileHandle.getLength(rid.pageNum, rid.slotNum);
	void *recordData = (void *)malloc(recordlen);
	readRecord(fileHandle, recordDescriptor, rid, recordData);
	printf("#### after readAttribute()...\n");

	unsigned int offset = 0;
	unsigned int namelenght, nrofelement, i;
	int intval;
	float floatval;
	nrofelement = (unsigned int) recordDescriptor.size();

	//------------------------//
	for (i = 0; i < nrofelement; i++) {
		if (strcmp(attributeName.c_str(), recordDescriptor[i].name.c_str())== 0) {
			switch (recordDescriptor[i].type) {
					case 0:
						memcpy(data, (char *) recordData + offset, sizeof(int));
						offset += sizeof(int);
						break;
					case 1:
						memcpy(data, (char *) recordData + offset, sizeof(float));
						offset += sizeof(float);
						break;
					case 2:
						memcpy(&namelenght, (char *) data + offset, sizeof(int));
						offset += sizeof(int);

						memcpy(data, (char *) recordData + offset, sizeof(namelenght));
						offset += namelenght;
						break;
					default:
						break;
			}
			break;
		} else {

			switch (recordDescriptor[i].type) {
			case 0:
				memcpy(&intval, (char *) recordData + offset, sizeof(int));
				cout << recordDescriptor[i].name << ": " << intval << "\t";
				offset += sizeof(int);
				break;
			case 1:
				memcpy(&floatval, (char *) recordData + offset, sizeof(float));
				cout << recordDescriptor[i].name << ": " << floatval << "\t";
				offset += sizeof(float);
				break;
			case 2: //string case
				//read name lengh//
				memcpy(&namelenght, (char *) recordData + offset, sizeof(int));
				//namelenght = *(int *)data + offset;
				offset += sizeof(int);
				//read the name//
				char *name = (char *) malloc(namelenght + 1);
				memset(name, '\0', namelenght + 1);
				memcpy(name, (char *) recordData + offset, namelenght);
				cout << recordDescriptor[i].name << ": " << name << "\t";
				offset += namelenght;
				break;
			}
		}
	}
	cout << endl;
	return 0;
}


RC RecordBasedFileManager::reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNum)
{
	//read
	void *buffer = (void *)malloc(PAGE_SIZE);
	void *bufferNew = (void *)malloc(PAGE_SIZE);
	memset(bufferNew, 0, PAGE_SIZE);

	printf("\n.....entering reorganizePage(): page.%d \n", pageNum);
	int rc = fileHandle.readPage(pageNum, buffer);
	if (rc != 0) {
		printf("failed to read page: %d\n", pageNum);
	}

	//re-organize
	int maxSlotNum = fileHandle.getLastSlotNum(pageNum);
	int shiftSize = 0;


	if(pageNum==3){
		reorganize_count++;
		printf("#### before re-organize: page Num is 3! No.%d time!\n", reorganize_count);
		for(int slotNum = 0; slotNum <= maxSlotNum; slotNum++)
		{
			int dataOff = fileHandle.getOffset(pageNum, slotNum);
			int res = printRecord(recordDescriptor, (char *) buffer + dataOff);
			assert(res==0);
		}
		printf("\n");
	}


	for(int slotNum = 0; slotNum < maxSlotNum; slotNum++)
	{
		int offset = fileHandle.getOffset(pageNum, slotNum);
		int offset_next = fileHandle.getOffset(pageNum, slotNum+1);

		int len = fileHandle.getLength(pageNum, slotNum);
		int len_next = fileHandle.getLength(pageNum, slotNum+1);

		if(fileHandle.isSlotDeleted(pageNum, slotNum)){
			//if(offset_next != offset){
				printf("###offset_next: %d\t, offset: %d, len: %d\n",offset_next, offset, len);
				printf("###slot %d is deleted!", slotNum);
				//shiftSize = (offset_next - offset);
				//modify the slot offset in slot directory
				memcpy((char *)buffer + offset, (char *)buffer + offset_next, len_next);
				rc = fileHandle.updateSlotOffset(pageNum, slotNum + 1, offset);
			//}
		}else{
			printf("###offset_next: %d\t, offset: %d, len: %d\n",offset_next, offset, len);
			if((offset_next - offset) < len){
				//printf("###offset_next: %d\t, offset: %d, len: %d\n",offset_next, offset, len);
			}
			assert((offset_next - offset) >= len);
			shiftSize = (offset_next - offset - len);
			if(shiftSize>0){
				printf("####### slot %d will be shift!", slotNum+1);
				//modify the slot offset in slot directory
				rc = fileHandle.updateSlotOffset(pageNum, slotNum + 1, offset + len);

				//move the data in page
				memcpy((char *)buffer + offset + len , (char *)buffer + offset_next, len_next);
			}
		}

		/*if(shiftSize > 0)
		{
			//modify the slot offset in slot directory
			rc = fileHandle.updateSlotOffset(pageNum, slotNum + 1, offset_next - shiftSize);

			//move the data in page
			memcpy((char *)buffer + offset_next - shiftSize, (char *)buffer + offset_next, len_next);
		}*/

	}
	//memcpy(buffer, buffer, PAGE_SIZE);

	if(pageNum==3){
		reorganize_count++;
		printf("#### page Num is 3! No.%d time!\n", reorganize_count);
		for(int slotNum = 0; slotNum <= maxSlotNum; slotNum++)
		{
			int dataOff = fileHandle.getOffset(pageNum, slotNum);
			int res = printRecord(recordDescriptor, (char *) buffer + dataOff);
			assert(res==0);
		}
		exit(-1);
	}


	//write back
//	rc = fileHandle.savePageInfo();
//	assert(rc == 0);

	rc = fileHandle.writePage(pageNum, buffer);
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

	int pageNum;
	int slotNum;

	int type;
//	int valuelen = 0;


	if(conditionAttribute=="" || compOp == NO_OP){
		for(pageNum=0; pageNum<fileHandle.getNumberOfPages(); pageNum ++)
		{
			for(slotNum=0; slotNum <= fileHandle.getLastSlotNum(pageNum); slotNum++)
			{
				if (!fileHandle.isSlotDeleted(pageNum, slotNum)) {
					RID rid;
					if (fileHandle.isSlotTombStone(pageNum, slotNum)) {
						fileHandle.getSlotTombStoneRID(pageNum, slotNum, rid);
					} else {
						rid.pageNum = pageNum;
						rid.slotNum = slotNum;
					}

					rbfm_ScanIterator.addPosition(rid);
				}
			}
		}
		printf("##### SEQUENCIAL SCAN \n");



		return 0;
	}


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
						//valuelen = recordDescriptor[i].length;
						break;
					default:
						break;
					}

		}
	}



	//retrieve each record in the file
	for(pageNum=0; pageNum<fileHandle.getNumberOfPages(); pageNum ++)
	{
		for(slotNum=0; slotNum <= fileHandle.getLastSlotNum(pageNum); slotNum++)
		{
			bool found = false;//if the record satisfies the requirement.
			if (!fileHandle.isSlotDeleted(pageNum, slotNum)) {
				RID rid;
				if (fileHandle.isSlotTombStone(pageNum, slotNum)) {
					fileHandle.getSlotTombStoneRID(pageNum, slotNum, rid);
				} else {
					rid.pageNum = pageNum;
					rid.slotNum = slotNum;
				}
				//void * conditionAttributeData = (void *)malloc(datalen);
				//readAttribute(fileHandle, recordDescriptor, rid, conditionAttribute, conditionAttributeData);
				//check if data satisfies the requirement
				if (type == 0) {
					int conditionAttributeData;
					readAttribute(fileHandle, recordDescriptor, rid,conditionAttribute, &conditionAttributeData);
					int attriValue;
					memcpy(&attriValue, (char *)value, sizeof(int));
					printf("conditionAttributeData: %d, value=%d\n", conditionAttributeData, attriValue);


					switch (compOp) {
					case EQ_OP:
						if (conditionAttributeData == attriValue ) {
							found = true;
						}
						break;
					case LT_OP:
						if (conditionAttributeData < attriValue) {
							found = true;
						}
						break;
					case GT_OP:
						if (conditionAttributeData > attriValue) {
							found = true;
						}
						break;
					case LE_OP:
						if (conditionAttributeData <= attriValue) {
							found = true;
						}
						break;
					case GE_OP:
						if (conditionAttributeData >= attriValue) {
							found = true;
						}
						break;
					case NE_OP:
						if (conditionAttributeData != attriValue) {
							found = true;
						}
						break;
					case NO_OP:
						found = true;
						break;
					}
				} else if (type == 1) {
					float conditionAttributeData;
					readAttribute(fileHandle, recordDescriptor, rid, conditionAttribute, &conditionAttributeData);

					float attriValue;
					memcpy(&attriValue, (char *)value, sizeof(float));
					printf("conditionAttributeData: %f, \n", conditionAttributeData);

					switch (compOp) {
					case EQ_OP:
						if (conditionAttributeData == attriValue) {
							found = true;
						}
						break;
					case LT_OP:
						if (conditionAttributeData < attriValue) {
							found = true;
						}
						break;
					case GT_OP:
						if (conditionAttributeData > attriValue) {
							found = true;
						}
						break;
					case LE_OP:
						if (conditionAttributeData <= attriValue) {
							found = true;
						}
						break;
					case GE_OP:
						if (conditionAttributeData >= attriValue) {
							found = true;
						}
						break;
					case NE_OP:
						if (conditionAttributeData != attriValue) {
							found = true;
						}
						break;
					case NO_OP:
						found = true;
						break;
					}
				} else {
					char *conditionAttributeData = (char *) malloc(30);
					memset((char *)conditionAttributeData, '\0', 30);
					readAttribute(fileHandle, recordDescriptor, rid,conditionAttribute, conditionAttributeData);
					char *attriValue = (char *) value;

					printf("conditionAttributeData: %s, value %s\n", conditionAttributeData, attriValue);
					switch (compOp) {
					case EQ_OP:
						if(strcmp(attriValue, conditionAttributeData) == 0) {
							found = true;
						}
						break;
					case LT_OP:
						if(strcmp(attriValue, conditionAttributeData) < 0) {
							found = true;
						}
						break;
					case GT_OP:
						if(strcmp(attriValue, conditionAttributeData) > 0) {
							found = true;
						}
						break;
					case LE_OP:
						if(strcmp(attriValue, conditionAttributeData) <= 0) {
							found = true;
						}
						break;
					case GE_OP:
						if(strcmp(attriValue, conditionAttributeData) >= 0) {
							found = true;
						}
						break;
					case NE_OP:
						if(strcmp(attriValue, conditionAttributeData) != 0) {
							found = true;
						}
						break;
					case NO_OP:
						found = true;
						break;
					}

					free(conditionAttributeData);
				}

				if (found) {
					printf("##### rid found %d, %d", rid.pageNum, rid.slotNum);
					rbfm_ScanIterator.addPosition(rid);
				}


			}
		}
	}


	return 0;
}
