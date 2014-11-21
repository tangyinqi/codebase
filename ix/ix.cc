
#include "ix.h"

Bucket::Bucket(int id, int numberOfPages)
{
	this->bucketId = id;
	this->entryMaxNumber = numberOfPages;
	last = 0;
	//overFlow = false;
}

Bucket::~Bucket()
{
	entrys.clear();
}

int Bucket::getFirstEntryPostion(EntryKey key)
{
	if(entryLookup.find(key)!=entryLookup.end())
		return entryLookup[key];
	return -1;
}

int Bucket::getBucketSize()
{
	return last;
}

//
RC Bucket::addEntry(Entry entry)
{
	if(last==entryMaxNumber){
		return PRIMEPAGES_FULL;
	}

	if(entryLookup.find(entry.key)==entryLookup.end())
	{
		entrys.push_back(entry);
		entryLookup[entry.key] = last;
	}else{
		int position = entryLookup[entry.key];
		entrys.insert(entry, position);
	}
	last++;

	return SUCCESS;
}

PrimeBucket::PrimeBucket(int id, int numberOfPages):Bucket(id, numberOfPages)
{
	overFlow = false;
}

PrimeBucket::~PrimeBucket()
{
}

bool PrimeBucket::isOverFlow()
{
	return overFlow;
}

void PrimeBucket::setOverFlow()
{
	overFlow = true;
}

void PrimeBucket::resetOverFlow()
{
	overFlow = false;
}
/*
OverflowBucket::OverflowBucket(int id, int numberOfPages):Bucket(id, numberOfPages)
{

}

OverflowBucket::~OverflowBucket()
{

}*/

MetaData::MetaData(int initialCapacity, int next, int level, int numberOfPagesEachBucket)
{
	this->initialCapacity = initialCapacity;
	this->next = next;
	this->level = level;
	this->numberOfPagesEachBucket = numberOfPagesEachBucket;
}

MetaData::~MetaData()
{

}

RC MetaData::increaseNext()
{
	unsigned N = initialCapacity << level;
	next++;
	if(next==N){
		next = 0;
		level++;
		return BUCKETS_FULL;
	}
	return SUCCESS;
}

int MetaData::getNext()
{
	return next;
}

int MetaData::getLevel()
{
	return level;
}

int MetaData::getInitialCapacity()
{
	return initialCapacity;
}

int MetaData::getBucketCapacity()
{
	return numberOfPagesEachBucket;
}


OverFLowPages::OverFLowPages(int numberOfPages)
{
	numberOfPagesEachBucket = numberOfPages;
}

OverFLowPages::~OverFLowPages()
{

}

RC OverFLowPages::addOverFlowPage(int primePageId, Entry entry)
{
	printf("add overflow page to %d primePage", primePageId);
	//check if hash_map contains key<primePageId>
	//if it contains:
	if(overFlowPages.find(primePageId)!=overFlowPages.end())
	{
		vector<Bucket*> buckets = overFlowPages[primePageId];
		//get last bucket of the list
		Bucket *bucket = buckets.back();
		//insert the entry into the bucket
		if(bucket->addEntry(entry)!=SUCCESS){
			//if the bucket is full
			printf("the overflow bucket is full!, a new overflowpage is appended");
			Bucket *nextBucket = new Bucket(primePageId, numberOfPagesEachBucket);
			nextBucket->addEntry(entry);
			buckets.push_back(nextBucket);
		}
	}else{
		Bucket *bucket = new Bucket(primePageId, numberOfPagesEachBucket);
		bucket->addEntry(entry);

		vector<Bucket*> buckets;
		buckets.push_back(bucket);

		overFlowPages[primePageId] = buckets;
	//if it doesn't contain
	}

	return SUCCESS;
}


LinearHash::LinearHash()
{

}

LinearHash::~LinearHash()
{
	primeBuckets.clear();
}

RC LinearHash::setMetaData(MetaData *meta)
{
	metaData = meta;
	return SUCCESS;
}

RC LinearHash::setPrimeBuckes(vector<PrimeBucket> primeBuckets)
{
	this->primeBuckets = primeBuckets;
}

PrimeBucket LinearHash::getBucket(int bucketId)
{
	//check if bucketId is within range of bucket
	if(bucketId< primeBuckets.size()){
		return primeBuckets[bucketId];
	}
	return null;
}

RC LinearHash::appendBucket(PrimeBucket bucket)
{
	int N = metaData->getInitialCapacity() << metaData->getLevel();
	if(N>primeBuckets.size()){
		primeBuckets.push_back(bucket);
		return SUCCESS;
	}
	return BUCKETS_FULL;
}

unsigned LinearHash::hash(const Attribute &attribute, const void *key)
{
	unsigned N = metaData->getInitialCapacity() << metaData->getLevel();
	unsigned hash_value;
	switch(attribute.type){
	case 0:
	case 1:
		hash_value = (unsigned)key % N;
		break;
	case 2:
		int len;
		memcpy(&len, (char *)key, sizeof(int));
		char *var = (char *)malloc(len);
		memcpy(var, (char *)key+sizeof(int), len);
		hash_value = 0;
		for(int i=0; i<len; i++)
		{
			hash_value *= 100;
			hash_value += var[i];
		}
		hash_value = hash_value % N;
		break;
	}

	return hash_value;

}

RC LinearHash::insertEntry(int primePageId, Entry entry)
{
	//check whether primePageId is within range

	//check passed
}

RC LinearHash::deleteEntry(int primePageId, Entry entry)
{

}

RC LinearHash::split()
{
	//triggered by some bucket is full,  strategy: round robin, controlled by "next"
	//split bucket next, and redistribtue the entrys to bucket next+N, append bucket next+N to buckets
}



IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName, const unsigned &numberOfPages)
{
	//pfm->createFile(fileName.c_str());
	//pfm->createFile(string(fileName).append(METADATA).c_str());

	//create hash index whose name is filename,
	//create numberOfPages buckets, and store it in the index
	if(FileExists(fileName.c_str())){
		return INDEX_EXISTS;
	}

	FILE *pIndexFile = fopen(fileName.c_str(), "r+b");
	if(pIndexFile==NULL){
		return FILE_OPEN_FAIL;
	}

	int offset =0;
	int dataLen = sizeof(PrimeBucket) * numberOfPages;
	void *data = (void*)malloc(dataLen);

	for(int i=0;i<numberOfPages;i++)
	{
		PrimeBucket *primePage= new PrimeBucket(i, numberOfPages);
		memcpy((char *)data + offset, primePage, sizeof(PrimeBucket));
		offset += sizeof(PrimeBucket);
	}

	//fseek(pIndexFile, offset, SEEK_SET);
	fwrite(data, dataLen, 1, pIndexFile);

	fclose(pIndexFile);


	if(FileExists(string(fileName).append(METADATA).c_str())){
		return METADATA_EXISTS;
	}

	FILE *pMetaDataFile = fopen(string(fileName).append(METADATA).c_str(), "r+b");
	if(pMetaDataFile==NULL){
		return FILE_OPEN_FAIL;
	}

	int next = 0;
	int level = 0;
	offset = 0;

	fseek(pMetaDataFile, offset, SEEK_SET);
	fwrite(&numberOfPages, sizeof(int), 1, pMetaDataFile);
	offset += sizeof(int);

	fseek(pMetaDataFile, offset, SEEK_SET);
	fwrite(&next, sizeof(int), 1, pMetaDataFile);
	offset += sizeof(int);

	fseek(pMetaDataFile, offset, SEEK_SET);
	fwrite(&level, sizeof(int), 1, pMetaDataFile);

	fclose(pMetaDataFile);
	return 0;
}

RC IndexManager::destroyFile(const string &fileName)
{
	return pfm->destroyFile(fileName.c_str());
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixFileHandle)
{
	return pfm->openFile(fileName.c_str(), ixFileHandle);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{

	return pfm->closeFile(ixfileHandle);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	return -1;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	return -1;
}

unsigned IndexManager::hash(const Attribute &attribute, const void *key)
{
	return 0;
}

RC IndexManager::printIndexEntriesInAPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const unsigned &primaryPageNumber) 
{
	return -1;
}

RC IndexManager::getNumberOfPrimaryPages(IXFileHandle &ixfileHandle, unsigned &numberOfPrimaryPages) 
{
	return -1;
}

RC IndexManager::getNumberOfAllPages(IXFileHandle &ixfileHandle, unsigned &numberOfAllPages) 
{
	return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
    const Attribute &attribute,
    const void      *lowKey,
    const void      *highKey,
    bool			lowKeyInclusive,
    bool        	highKeyInclusive,
    IX_ScanIterator &ix_ScanIterator)
{
	return -1;
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	return -1;
}

RC IX_ScanIterator::close()
{
	return -1;
}


IXFileHandle::IXFileHandle()
{
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	return -1;
}

void IX_PrintError (RC rc)
{
}
