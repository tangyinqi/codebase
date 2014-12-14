
#include "ix.h"


Bucket::Bucket()
{
	entryMaxNumber = 20;
	last = 0;
	overFlow = false;
}

Bucket::Bucket(int id)
{
	this->bucketId = id;
//	this->entryMaxNumber = numberOfPages;
	entryMaxNumber = 20;
	last = 0;
	overFlow = false;
}

Bucket::~Bucket()
{
	entrys.clear();
}

int Bucket::getFirstEntryPostion(const Attribute &attribute, const void *key)
{
	switch(attribute.type){
	case 0:
	{
			int key1;
			memcpy(&key1, (char *)key, sizeof(int));
			if(entryLookup1.find(key1)!=entryLookup1.end())
				return entryLookup1[key1];
			return -1;
	}
	case 1:
	{
		float key2;
		memcpy(&key2, (char *)key, sizeof(float));
		if(entryLookup2.find(key2)!=entryLookup2.end())
			return entryLookup2[key2];
		return -1;
	}
	case 2:
	{
		int len;
		memcpy(&len, (char *)key, sizeof(int));
		char *key3 = (char *)malloc(len+1);
		memset(key3, '\0', len+1);
		memcpy(key3, (char *)key + 4, len);
		if(entryLookup3.find(key3)!=entryLookup3.end())
			return entryLookup3[key3];
		return -1;
	}
	default:
		printf("type undefined\n");
		return -1;

	}
}

RC Bucket::deleteEntry(const Attribute &attribute, const void *key, const RID &rid)
{
	int start = getFirstEntryPostion(attribute, key);
	Entry entry = getEntry(start);
	int index;
	if((entry.rid.pageNum == rid.pageNum ) && (entry.rid.slotNum == rid.slotNum))
	{
		index = start;
	}else{
		for(int i=start+1; i<last; i++){
				entry = getEntry(i);
				if(attribute.type == 0){
					int key1, key2;
					key1 = entry.key.key1;
		//			memcpy(&key1, (char *) entry.key.key1, sizeof(int));
					memcpy(&key2, (char *) key, sizeof(int));
					if ((key1 == key2) && (entry.rid.pageNum == rid.pageNum)
							&& (entry.rid.slotNum == rid.slotNum)) {
						index = i;
						break;
					}
				}else if(attribute.type == 1){
					float key1, key2;
					key1 = entry.key.key2;
		//			memcpy(&key1, (char *)entry.key.key2, sizeof(float));
					memcpy(&key2, (char *)key, sizeof(float));
					if((key1 == key2) && (entry.rid.pageNum == rid.pageNum ) && (entry.rid.slotNum == rid.slotNum))
					{
						index = i;
						break;
					}
				}else if(attribute.type == 2){
					if((strcmp((char *)key, (char *)entry.key.key3) == 0) && (entry.rid.pageNum == rid.pageNum ) && (entry.rid.slotNum == rid.slotNum))
					{
						index = i;
						break;
					}
				}

			}
	}



	if(index==last)
	{
		return ENTRY_NOT_EXIST;
	}

	vector<Entry> entryBackup;
	for(int i=0; i<last; i++)
	{
		if(i!=index){
			entryBackup.push_back(getEntry(i));
		}
	}

	entrys.clear();
	entryLookup1.clear();
	entryLookup2.clear();
	entryLookup3.clear();

	for(int i=0; i< entryBackup.size(); i++)
	{
		Entry entry2 = entryBackup[i];
//		entry2.key = entryBackup[i].key;
//		entry2.rid.pageNum = entryBackup[i].rid.pageNum;
//		entry2.rid.slotNum = entryBackup[i].rid.slotNum;
		addEntry(entry2, attribute.type);
	}
	last--;
	return SUCCESS;
}


int Bucket::getBucketSize()
{
	return last;
}

RC Bucket::rangeScan(const Attribute &attribute,
		  const void        *lowKey,
		  const void        *highKey,
		  bool        lowKeyInclusive,
		  bool        highKeyInclusive,
		  IX_ScanIterator &ix_ScanIterator)
{
	cout<<"##Bucket::rangeScan()....no."<<bucketId<<endl;
	for (int i = 0; i < last; i++) {
		Entry entry = getEntry(i);
		if (lowKey != NULL && highKey != NULL) {
			if (attribute.type == 0) {
				int low, high, key;
				key = entry.key.key1;
//				memcpy(&key, (char *) entry.key, sizeof(int));
				memcpy(&low, (char *) lowKey, sizeof(int));
				memcpy(&high, (char *) highKey, sizeof(int));
				if (lowKeyInclusive && highKeyInclusive) {
					if ((key >= low) && (key <= high)) {
						ix_ScanIterator.addEntry(entry);
					}
				} else if (lowKeyInclusive) {
					if ((key >= low) && (key < high)) {
						ix_ScanIterator.addEntry(entry);
					}
				} else if (highKeyInclusive) {
					if ((key > low) && (key <= high)) {
						ix_ScanIterator.addEntry(entry);
					}
				} else {
					if ((key > low) && (key < high)) {
						ix_ScanIterator.addEntry(entry);
					}
				}
			} else if (attribute.type == 1) {
				float low, high, key;
				key = entry.key.key2;
//				memcpy(&key, (char *) entry.key, sizeof(float));
				memcpy(&low, (char *) lowKey, sizeof(float));
				memcpy(&high, (char *) highKey, sizeof(float));
				if (lowKeyInclusive && highKeyInclusive) {
					if ((key >= low) && (key <= high)) {
						ix_ScanIterator.addEntry(entry);
					}
				} else if (lowKeyInclusive) {
					if ((key >= low) && (key < high)) {
						ix_ScanIterator.addEntry(entry);
					}
				} else if (highKeyInclusive) {
					if ((key > low) && (key <= high)) {
						ix_ScanIterator.addEntry(entry);
					}
				} else {
					if ((key > low) && (key < high)) {
						ix_ScanIterator.addEntry(entry);
					}
				}
			} else if (attribute.type == 2) {
				int len1, len2, len3;
				memcpy(&len1, (char *) lowKey, sizeof(int));
				memcpy(&len2, (char *) highKey, sizeof(int));
				memcpy(&len3, (char *) entry.key.key3, sizeof(int));

				char *low = (char *) malloc(len1 + 1);
				memset(low, '\0', len1 + 1);

				char *high = (char *) malloc(len2 + 1);
				memset(high, '\0', len2 + 1);

				char *var = (char *) malloc(len3 + 1);
				memset(var, '\0', len3 + 1);

				if (lowKeyInclusive && highKeyInclusive) {
					if (strcmp(var, low) >= 0 && strcmp(var, high) <= 0) {
						ix_ScanIterator.addEntry(entry);
					}
				} else if (lowKeyInclusive) {
					if (strcmp(var, low) >= 0 && strcmp(var, high) < 0) {
						ix_ScanIterator.addEntry(entry);
					}
				} else if (highKeyInclusive) {
					if (strcmp(var, low) > 0 && strcmp(var, high) <= 0) {
						ix_ScanIterator.addEntry(entry);
					}
				} else {
					if (strcmp(var, low) > 0 && strcmp(var, high) < 0) {
						ix_ScanIterator.addEntry(entry);
					}
				}
			}

		} else if (lowKey != NULL) {
			if (attribute.type == 0) {
				int low, high, key;
				key = entry.key.key1;
//				memcpy(&key, (char *) entry.key, sizeof(int));
				memcpy(&low, (char *) lowKey, sizeof(int));
				//memcpy(&high, (char *) highKey, sizeof(int));
				if (lowKeyInclusive) {
					if (key >= low) {
						ix_ScanIterator.addEntry(entry);
					}
				} else {
					if (key > low) {
						ix_ScanIterator.addEntry(entry);
					}
				}
			} else if (attribute.type == 1) {
				float low, high, key;
				key = entry.key.key2;
//				memcpy(&key, (char *) entry.key, sizeof(float));
				memcpy(&low, (char *) lowKey, sizeof(float));
				//memcpy(&high, (char *) highKey, sizeof(float));
				if (lowKeyInclusive) {
					if (key >= low) {
						ix_ScanIterator.addEntry(entry);
					}
				} else {
					if (key > low) {
						ix_ScanIterator.addEntry(entry);
					}
				}
			} else if (attribute.type == 2) {
				int len1, len2, len3;
				memcpy(&len1, (char *) lowKey, sizeof(int));
				//memcpy(&len2, (char *)highKey, sizeof(int));
				memcpy(&len3, (char *) entry.key.key3, sizeof(int));

				char *low = (char *) malloc(len1 + 1);
				memset(low, '\0', len1 + 1);

//				char *high = (char *)malloc(len2+1);
//				memset(high, '\0', len2+1);

				char *var = (char *) malloc(len3 + 1);
				memset(var, '\0', len3 + 1);

				if (lowKeyInclusive) {
					if (strcmp(var, low) >= 0) {
						ix_ScanIterator.addEntry(entry);
					}
				} else {
					if (strcmp(var, low) > 0) {
						ix_ScanIterator.addEntry(entry);
					}
				}
			}
		} else if (highKey != NULL) {
			if (attribute.type == 0) {
				int low, high, key;
				key = entry.key.key1;
//				memcpy(&key, (char *) entry.key, sizeof(int));
				//memcpy(&low, (char *) lowKey, sizeof(int));
				memcpy(&high, (char *) highKey, sizeof(int));

				if (highKeyInclusive) {
					if (key <= high) {
						ix_ScanIterator.addEntry(entry);
					}
				} else {
					if (key < high) {
						ix_ScanIterator.addEntry(entry);
					}
				}
			} else if (attribute.type == 1) {
				float low, high, key;
				key = entry.key.key2;
//				memcpy(&key, (char *) entry.key, sizeof(float));
				//memcpy(&low, (char *) lowKey, sizeof(float));
				memcpy(&high, (char *) highKey, sizeof(float));

				if (highKeyInclusive) {
					if (key <= high) {
						ix_ScanIterator.addEntry(entry);
					}
				} else {
					if (key < high) {
						ix_ScanIterator.addEntry(entry);
					}
				}
			} else if (attribute.type == 2) {
				int len1, len2, len3;
				//memcpy(&len1, (char *)lowKey, sizeof(int));
				memcpy(&len2, (char *) highKey, sizeof(int));
				memcpy(&len3, (char *) entry.key.key3, sizeof(int));

//				char *low = (char *)malloc(len1+1);
//				memset(low, '\0', len1+1);

				char *high = (char *) malloc(len2 + 1);
				memset(high, '\0', len2 + 1);

				char *var = (char *) malloc(len3 + 1);
				memset(var, '\0', len3 + 1);

				if (highKeyInclusive) {
					if (strcmp(var, high) <= 0) {
						ix_ScanIterator.addEntry(entry);
					}
				} else {
					if (strcmp(var, high) < 0) {
						ix_ScanIterator.addEntry(entry);
					}
				}
			}
		} else {
			ix_ScanIterator.addEntry(entry);
		}

	}


	return SUCCESS;
}


RC Bucket::equalityScan(const Attribute &attribute, const void *key, IX_ScanIterator &ix_ScanIterator)
{
	int start =  getFirstEntryPostion(attribute, key);

	if (start == -1) {
		return KEY_NOT_FOUNT;
	}
	//
	Entry entry = getEntry(start);
	ix_ScanIterator.addEntry(entry);

//	ix_ScanIterator.setType(attribute.type);

	for (int i = start + 1; i < last; i++) {
		entry = getEntry(i);
		if (attribute.type == 0) {
			int key1, key2;
			key1 = entry.key.key1;
//			memcpy(&key1, (char *) entry.key, sizeof(int));
			memcpy(&key2, (char *) key, sizeof(int));
			if (key1 == key2) {
				ix_ScanIterator.addEntry(entry);
			}
		} else if (attribute.type == 1) {
			float key11, key22;
			key11 = entry.key.key1;
//			memcpy(&key11, (char *) entry.key, sizeof(float));
			memcpy(&key22, (char *) key, sizeof(float));
			if (key11 == key22) {
				ix_ScanIterator.addEntry(entry);
			}
		} else if (attribute.type == 2) {
			int len1, len2;
			memcpy(&len1, (char *) entry.key.key3, sizeof(int));
			memcpy(&len2, (char *)key, sizeof(int));
			if(len1 == len2){
				char *var1 = (char *)malloc(len1 + 1);
				memset(var1, '\0', len1 + 1);

				char *var2 = (char *)malloc(len2 + 1);
				memset(var2, '\0', len2 + 1);
				if(strcmp(var1, var2) == 0){
					ix_ScanIterator.addEntry(entry);
				}
			}
		}
	}

	return SUCCESS;
}

RC Bucket::addEntry(Entry entry, int type)
{

	if(last==entryMaxNumber){
		return PRIMEPAGES_FULL;
	}

//	cout<<"##addEntry(): bucketId = "<<bucketId<<", last = "<<last<<endl;
//	bucketId++;
	EntryKey key;

	if(type == 0)
	{
		int key1 = entry.key.key1;
//		memcpy(&key1, (char *)entry.key, sizeof(int));
//		cout<<"key:"<<entry.key.key1<<"/"<<entry.rid.pageNum<<","<<entry.rid.slotNum<<endl;
		if(entryLookup1.find(key1)==entryLookup1.end())
		{
			entrys.push_back(entry);
			entryLookup1[key1] = last;
		}else{
			entrys.push_back(entry);
			//int position = entryLookup[key.key1];
			//vector<Entry>::iterator it = entrys.begin();
			//entrys.insert(it + position, entry);
		}

	}else if(type ==1){
		float key2 = entry.key.key2;
//		memcpy(&key2, (char *)entry.key, sizeof(float));
//		cout<<"key:"<<entry.key.key2<<"/"<<entry.rid.pageNum<<","<<entry.rid.slotNum<<endl;
		if(entryLookup2.find(key2)==entryLookup2.end())
		{
			entrys.push_back(entry);
			entryLookup2[key2] = last;
		}else{
			entrys.push_back(entry);
//			int position = entryLookup[key.key2];
//			vector<Entry>::iterator it = entrys.begin();
//			entrys.insert(it + position, entry);
		}

	}else if(type ==2){
		int len;
		memcpy(&len, (char *)entry.key.key3, sizeof(int));

		char * key3 = (char *)malloc(len +1);
		memset(key3, '\0', len+1);
		memcpy(key3, (char *)entry.key.key3 + 4, len);
//		cout<<"key:"<<key3<<"/"<<entry.rid.pageNum<<","<<entry.rid.slotNum<<endl;;
		if(entryLookup3.find(key3)==entryLookup3.end())
		{
			entrys.push_back(entry);
			entryLookup3[key3] = last;
		}else{
			entrys.push_back(entry);
//			int position = entryLookup[key.key3];
//			vector<Entry>::iterator it = entrys.begin();
//			entrys.insert(it + position, entry);
		}
	}else{
		printf("type undefined!\n");

	}

	last++;

	return SUCCESS;
}

Entry Bucket::getEntry(int id)
{
	return entrys[id];
}

bool Bucket::isEmpty()
{
	return last==0;
}

int Bucket::getBucketId()
{
	return bucketId;
}

int Bucket::getLast()
{
	return last;
}

int Bucket::getBucketCapacity()
{
	return entryMaxNumber;
}


bool Bucket::isOverFlow()
{
	return overFlow;
}

void Bucket::setOverFlow()
{
	overFlow = true;
}

void Bucket::resetOverFlow()
{
	overFlow = false;
}

void Bucket::print(const Attribute &attribute)
{
	cout<<"Page No."<<bucketId<<endl;
	cout<<"# of entries: "<<last<<endl;
	if(last!=0){
		cout<<"entries:";
	}
	for(int i=0;i<last;i++)
	{
		switch(attribute.type){
		case 0:
			int key1;
			key1 = entrys[i].key.key1;
			cout<<"[key:"<<key1<<'/';
			break;
		case 1:
			float key2;
			key2 = entrys[i].key.key2;
//			memcpy(&key, (char *)entrys[i].key, sizeof(int));
			cout<<"[key:"<<key2<<'/';
			break;
		case 2:
			int len;
			memcpy(&len, (char *)entrys[i].key.key3, sizeof(int));
			char *var = (char *)malloc(len+1);
			memcpy(var, (char *)entrys[i].key.key3 + sizeof(int), len);
			cout<<"[key:"<<var<<'/';
			break;
		}
		cout<<"rid:"<<entrys[i].rid.pageNum<<","<<entrys[i].rid.slotNum<<']';
	}
	cout<<endl;
}

PrimeBucket::PrimeBucket(int id):Bucket(id)
{
//	Bucket(id, numberOfPages);
	this->bucketId = id;
	this->entryMaxNumber = 4;
	last = 0;

	overFlow = false;
}

PrimeBucket::~PrimeBucket()
{
}

int PrimeBucket::getBucketSize()
{
	return last;
}

Entry PrimeBucket::getEntry(int id)
{
	return entrys[id];
}

/*
OverflowBucket::OverflowBucket(int id, int numberOfPages):Bucket(id, numberOfPages)
{

}

OverflowBucket::~OverflowBucket()
{

}*/

MetaData::MetaData(int initialCapacity, unsigned next, int level, int numberOfPages)
{
	this->initialCapacity = initialCapacity;
	this->next = next;
	this->level = level;
	this->numberOfPages = numberOfPages;
	this->type = -1;
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

RC MetaData::decreaseNext()
{
	unsigned N = initialCapacity << (level -1);
	next--;
	if(next == (N-1)){
		level--;
	}
	return SUCCESS;
}

const unsigned MetaData::getNext()
{
	return next;
}

const int MetaData::getLevel()
{
	return level;
}

const int MetaData::getInitialCapacity()
{
	return initialCapacity;
}

const int MetaData::getPageNum()
{
	return numberOfPages;
}

const int MetaData::getType()
{
	return type;
}

void MetaData::setType(int Type)
{
	type = Type;
}

OverFLowPages::OverFLowPages()
{
	//numberOfPagesEachBucket = numberOfPages;
}

OverFLowPages::~OverFLowPages()
{

}

RC OverFLowPages::addOverFlowPage(int primePageId, Entry entry, const Attribute &attribute)
{
	printf("#addOverFlowPage() to %d primePage", primePageId);
	//check if hash_map contains key<primePageId>
	//if it contains:
	if(overFlowPages.find(primePageId)!=overFlowPages.end())
	{
		vector<Bucket*> buckets = overFlowPages.at(primePageId);
		//get last bucket of the list
		Bucket *bucket = buckets.back();
		//insert the entry into the bucket
		if(bucket->addEntry(entry, attribute.type)!=SUCCESS){
			//if the bucket is full
			printf("the overflow bucket is full!, a new overflowpage is appended");
			//Bucket *nextBucket = new Bucket(primePageId, numberOfPagesEachBucket);
			Bucket *nextBucket = new Bucket(primePageId);
			nextBucket->addEntry(entry, attribute.type);
			buckets.push_back(nextBucket);
		}
		overFlowPages[primePageId] = buckets;
	}else{
		//Bucket *bucket = new Bucket(primePageId, numberOfPagesEachBucket);
		Bucket *bucket = new Bucket(primePageId);
		bucket->addEntry(entry, attribute.type);

		vector<Bucket*> buckets;
		buckets.push_back(bucket);

		overFlowPages[primePageId] = buckets;
	//if it doesn't contain
	}

	return SUCCESS;
}

RC OverFLowPages::buildOverFlowHashMap(int primePageId, vector<Bucket*> buckets)
{
	overFlowPages[primePageId] = buckets;
	return SUCCESS;
}

vector<int>* OverFLowPages::getKeySet()
{
	vector<int> *keys = new vector<int>();
	for(unordered_map<int, vector<Bucket*> > ::iterator it = overFlowPages.begin(); it !=overFlowPages.end(); it++ )
	{
		keys->push_back(it->first);
	}

	return keys;
}

vector<Bucket*> OverFLowPages::getValue(int primePageId)
{
	return overFlowPages.at(primePageId);
}

void OverFLowPages::deleteValue(int primePageId)
{
	overFlowPages.erase(primePageId);
}

LinearHash::LinearHash()
{
	primeBucketNumber = 0;
	splitTriggerred = false;
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


RC LinearHash::setOverFlowPages(OverFLowPages *overFlowPages)
{
	this->overFlowPages = overFlowPages;
	return SUCCESS;
}

RC LinearHash::setPrimeBuckes(vector<Bucket *> primeBuckets)
{
	this->primeBuckets = primeBuckets;
	return SUCCESS;
}

Bucket *LinearHash::getBucket(unsigned bucketId)
{
	//check if bucketId is within range of bucket
	//if(bucketId< primeBuckets.size()){
	return primeBuckets.at(bucketId);

}

RC LinearHash::appendBucket(Bucket *bucket)
{
//	int N = metaData->getInitialCapacity() << metaData->getLevel();
//	if(N>primeBuckets.size()){
//		primeBuckets.push_back(bucket);
//		return SUCCESS;
//	}
	primeBuckets.push_back(bucket);
	primeBucketNumber++;

	return SUCCESS;
}

unsigned LinearHash::hash(const Attribute &attribute, const void *key, int level)
{
	unsigned N = metaData->getInitialCapacity() << level;
	//unsigned N = metaData->getInitialCapacity() << metaData->getLevel();
	cout<<"## hash(): level = "<<level<<", N = "<<N;
	unsigned hash_value;
	switch(attribute.type){
	case 0:
	case 1:
//		hash_value = key.key1;
		memcpy(&hash_value, (char *)key, sizeof(int));
		hash_value = hash_value % N;
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
	cout<<", hash_value = "<<hash_value<<endl;
	return hash_value;
}

unsigned LinearHash::hash(const Attribute &attribute, EntryKey key, int level)
{
	unsigned N = metaData->getInitialCapacity() << level;
	//unsigned N = metaData->getInitialCapacity() << metaData->getLevel();
	cout<<"## hash(): level = "<<level<<", N = "<<N;
	unsigned hash_value;
	switch(attribute.type){
	case 0:
	case 1:
		hash_value = key.key1;
//		memcpy(&hash_value, (char *)key, sizeof(int));
		hash_value = hash_value % N;
		break;
	case 2:
		int len;
		memcpy(&len, (char *)key.key3, sizeof(int));
		char *var = (char *)malloc(len);
		memcpy(var, (char *)key.key3+sizeof(int), len);
		hash_value = 0;
		for(int i=0; i<len; i++)
		{
			hash_value *= 100;
			hash_value += var[i];
		}
		hash_value = hash_value % N;
		break;
	}
	cout<<", hash_value = "<<hash_value<<endl;
	return hash_value;
}

unsigned LinearHash::lookforBucket(const Attribute &attribute, const void *key)
{
	unsigned next = metaData->getNext();
	int level = metaData->getLevel();
	unsigned bucketId = hash(attribute, key, level);

	cout<<"##lookforBucket(): next = "<<next<<", level = "<<level;

	if(bucketId >= next  && bucketId < primeBucketNumber ){
		cout<<", bucketId = "<<bucketId<<endl;
		return bucketId;
	}else{
		bucketId = hash(attribute, key, level + 1);
		cout<<", bucketId = "<<bucketId<<endl;
		return bucketId;
	}

}

bool LinearHash::isSplitTriggerred()
{
	return splitTriggerred;
}

RC LinearHash::insertEntry(unsigned bucketId, const Attribute &attribute, Entry entry)
{
	if(metaData->getType()==-1){
		metaData->setType(attribute.type);
	}
	//check whether primePageId is within range
	if(bucketId >= primeBucketNumber)
	{
		return PAGEID_EXCEEDS;
	}
	Bucket *p = getBucket(bucketId);
	int bucketCapacity = p->getBucketCapacity();
	if(p->getBucketSize() < bucketCapacity)
	{
//		cout<<"last="<<p->getLast()<<endl;
		p->addEntry(entry, attribute.type);
//		primeBuckets.[bucketId] = p;
	}else{
		if(!p->isOverFlow()){
			p->setOverFlow();
			if(!splitTriggerred){
				splitTriggerred = true;
			}
			//primeBuckets[bucketId] = p;
		}
		int rc = overFlowPages->addOverFlowPage(bucketId, entry, attribute);
		assert(rc==SUCCESS);
	}

	if(splitTriggerred){
		cout<<"##splitTriggerred..."<<endl;
		split(attribute);
	}
	return SUCCESS;
	//check passed
}

RC LinearHash::deleteEntry(unsigned primePageId, const Attribute &attribute, const void *key, const RID &rid)
{
	Bucket *bucket = getBucket(primePageId);
	int rc = bucket->deleteEntry(attribute, key, rid);
	assert(rc == 0);

	if(bucket->isOverFlow()){
		vector<Bucket *> buckets = overFlowPages->getValue(primePageId);
		for(int i=0; i< buckets.size(); i++){
			buckets[i]->deleteEntry(attribute, key, rid);
			if(buckets[i]->getLast()==0){
				buckets.erase(buckets.begin()+i);
			}
		}
		if(buckets.empty()){
			bucket->resetOverFlow();
		}
	}

	if(primeBuckets.at(primeBucketNumber-1)->isEmpty() && !primeBuckets.at(primeBucketNumber-1)->isOverFlow()){
		primeBucketNumber--;
		metaData->decreaseNext();
	}

//	int entryId = bucket.getFirstEntryPostion(attribute, key);
	return 0;
}

RC LinearHash::split(const Attribute &attribute)
{
	//triggered by some bucket is full,  strategy: round robin, controlled by "next"
	//split bucket next, and redistribtue the entrys to bucket next+N, append bucket next+N to buckets
	int next = metaData->getNext();
	int level = metaData->getLevel();

	Bucket *splitImage = new Bucket(primeBucketNumber);
//	splitImage.setId(primeBucketNumber);
//	splitImage.setCapacity(metaData->getBucketCapacity());
	appendBucket(splitImage);

	//redistribute the bucket next
	Bucket *toReDistribute = getBucket(next);

	cout<<"###before re-distribute"<<endl;
	toReDistribute->print(attribute);

	Bucket *oldImage = new Bucket(next);
//	oldImage.setId(toReDistribute.getBucketId());
//	oldImage.setCapacity(metaData->getBucketCapacity());

	//iterate the entry and re-calculate the key of each entry again
	vector<Entry> entrys;
	for(int id=0; id < toReDistribute->getBucketSize(); id++){
		Entry entry = toReDistribute->getEntry(id);
		entrys.push_back(entry);
	}
	cout<<"redistribute primary bucket finished!"<<endl;

	if(toReDistribute->isOverFlow()){
		vector<Bucket*> buckets = overFlowPages->getValue(next);
		for(int i=0; i < buckets.size();i++)
		{
			Bucket *p = buckets[i];
			cout<<"overflow page: ";
			p->print(attribute);

			for(int j=0; j<p->getBucketSize();j++)
			{
				Entry entry = p->getEntry(j);
				entrys.push_back(entry);
			}
		}
		cout<<"redistribute overflow buckets finished!"<<endl;
		//buckets.clear();
		overFlowPages->deleteValue(next);
	}

	primeBuckets[next] = oldImage;
	//re-distribute
	cout<<"entrys size: "<<entrys.size()<<endl;
	for(int i=0; i<entrys.size(); i++)
	{
		Entry entry = entrys.at(i);
		cout<<"  No. "<<i<<", "<<next<<endl;
		int bucketId = hash(attribute, entry.key, level + 1);
		if (bucketId == next) {
			//insertEntry(bucketId, attribute, entry);
			printf("entry moved to old bucket: %d\n", oldImage->getBucketId());
			if(oldImage->addEntry(entry, attribute.type)!=SUCCESS){
				overFlowPages->addOverFlowPage(oldImage->getBucketId(), entry, attribute);
				oldImage->setOverFlow();
			}

		}else{
			//insertEntry(bucketId, attribute, entry);
			printf("entry moved to new bucket: %d\n", splitImage->getBucketId());
			if(splitImage->addEntry(entry, attribute.type)){
				overFlowPages->addOverFlowPage(splitImage->getBucketId(), entry, attribute);
				splitImage->setOverFlow();
			}

		}

	}
//	exit(-1);

	cout<<"after re-distribution: "<<endl;
	oldImage->print(attribute);
	//if()
	splitImage->print(attribute);


	metaData->increaseNext();
	return SUCCESS;
}

RC LinearHash::rangeScan(const Attribute &attribute,
		  const void        *lowKey,
		  const void        *highKey,
		  bool        lowKeyInclusive,
		  bool        highKeyInclusive,
		  IX_ScanIterator &ix_ScanIterator)
{
	ix_ScanIterator.setType(attribute.type);

	cout<<"###rangeScan()....."<<endl;

	for(int i=0; i < primeBucketNumber; i++)
	{
		Bucket *bucket = primeBuckets[i];

		bucket->print(attribute);

		bucket->rangeScan(attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, ix_ScanIterator);
		if(bucket->isOverFlow()){
			cout<<"$$$ scanning over flow pages $$$";
			vector<Bucket*> buckets = overFlowPages->getValue(i);
			for(int i=0; i<buckets.size(); i++){

				buckets[i]->print(attribute);

				buckets[i]->rangeScan(attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, ix_ScanIterator);
			}
		}
	}
	return 0;
}

RC LinearHash::equalityScan(const Attribute &attribute, const void *Key,
		IX_ScanIterator &ix_ScanIterator)
{
	ix_ScanIterator.setType(attribute.type);
	cout<<"##equalityScan()..."<<endl;
	unsigned bucketId = lookforBucket(attribute, Key);
	Bucket *bucket = getBucket(bucketId);

	bucket->equalityScan(attribute, Key, ix_ScanIterator);

	if(bucket->isOverFlow()){
		vector<Bucket*> buckets = overFlowPages->getValue(bucketId);
		cout<<"overflow page size:"<<buckets.size()<<endl;
		for(int i=0; i<buckets.size(); i++){
			buckets[i]->equalityScan(attribute, Key, ix_ScanIterator);
		}
	}

	return 0;
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

	FILE *pIndexFile = fopen(fileName.c_str(), "w");

	if(pIndexFile!=NULL){
		fclose(pIndexFile);
	}

	pIndexFile = fopen(fileName.c_str(), "r+b");
	if(pIndexFile==NULL){
		return FILE_OPEN_FAIL;
	}

	int offset =0;
	int dataLen = sizeof(Bucket) * numberOfPages;
	void *data = (void*)malloc(dataLen);

	for(int i=0;i<numberOfPages;i++)
	{
		Bucket *primePage= new Bucket(i);
		memcpy((char *)data + offset, primePage, sizeof(Bucket));
		offset += sizeof(Bucket);
	}

	//fseek(pIndexFile, offset, SEEK_SET);
	fwrite(data, dataLen, 1, pIndexFile);

	fclose(pIndexFile);


	if(FileExists(string(fileName).append(METADATA).c_str())){
		return METADATA_EXISTS;
	}

	FILE *pMetaData = fopen(string(fileName).append(METADATA).c_str(), "w");

	if(pMetaData!=NULL){
		fclose(pMetaData);
	}

	pMetaData = fopen(string(fileName).append(METADATA).c_str(), "r+b");
	if(pMetaData==NULL){
		return FILE_OPEN_FAIL;
	}

	unsigned next = 0;
	int level = 0;
	int type = -1;
	offset = 0;

	//initial_capacity
	fseek(pMetaData, offset, SEEK_SET);
	fwrite(&numberOfPages, sizeof(unsigned), 1, pMetaData);
	offset += sizeof(unsigned);

	//number of pages
	fseek(pMetaData, offset, SEEK_SET);
	fwrite(&numberOfPages, sizeof(unsigned), 1, pMetaData);
	offset += sizeof(unsigned);

	fseek(pMetaData, offset, SEEK_SET);
	fwrite(&next, sizeof(unsigned), 1, pMetaData);
	offset += sizeof(unsigned);

	fseek(pMetaData, offset, SEEK_SET);
	fwrite(&level, sizeof(int), 1, pMetaData);
	offset += sizeof(int);

	fseek(pMetaData, offset, SEEK_SET);
	fwrite(&type, sizeof(int), 1, pMetaData);
	offset += sizeof(int);

	int flowpagesize = 0;
	fseek(pMetaData, offset, SEEK_SET);
	fwrite(&type, sizeof(int), 1, pMetaData);

	fclose(pMetaData);
	return 0;
}

RC IndexManager::destroyFile(const string &fileName)
{
	if(!FileExists(fileName.c_str())) return -1;

	int rc = remove(fileName.c_str());
	assert(rc ==0);

	rc = remove(string(fileName).append(METADATA).c_str());
	assert(rc ==0);

	return SUCCESS;
	//return PagedFileManager::instance()->destroyFile(fileName.c_str());
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixFileHandle)
{
	int rc;
	if(!FileExists(fileName.c_str()))return -1;

	FILE *pIndex = fopen(fileName.c_str(), "r+b");
	if(pIndex!=NULL){
		int rc = ixFileHandle.setIndexHandle(pIndex);
		assert(rc == 0);
	}

	FILE *pMetaData = fopen(string(fileName).append(METADATA).c_str(), "r+b");

	if(pMetaData!=NULL)
	{
		rc = ixFileHandle.setMedatDataHandle(pMetaData);
		assert(rc == 0);
	}

	 rc = ixFileHandle.readMetaData();
	 assert(rc == 0);

	 rc = ixFileHandle.readPrimePages();
	 assert(rc == 0);

	 rc = ixFileHandle.readFlowPages();
	 assert(rc == 0);


	 return SUCCESS;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	int rc;
	rc = ixfileHandle.writePrimePages();
	assert(rc == SUCCESS);

	rc = ixfileHandle.writeMetaData();
	assert(rc == SUCCESS);

	rc = ixfileHandle.writeFlowPages();
	assert(rc == SUCCESS);

	rc = ixfileHandle.closeHandle();
	assert(rc == SUCCESS);

	return SUCCESS;
	//return pfm->closeFile(ixfileHandle);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	Entry entry;

	switch(attribute.type)
	{
	case 0:
		int key1;
		memcpy(&key1, (char *)key, 4);
		entry.key.key1 = key1;
//		entry.key = key1;
		break;
	case 1:
		float key2;
		memcpy(&key2, (char *)key, 4);
		entry.key.key2 = key2;
//		entry.key = key2;
		break;
	case 2:
		int len;
		memcpy(&len, (char *)key, 4);

		entry.key.key3 = (char *)malloc(4 + len);
		memcpy((char *)entry.key.key3, (char *)key, 4+len);
		break;
	}

	if(ixfileHandle.getLinearHash()->getMetaData()->getType()==-1){
		ixfileHandle.getLinearHash()->getMetaData()->setType(attribute.type);
	}

	entry.rid.pageNum = rid.pageNum;
	entry.rid.slotNum = rid.slotNum;

	unsigned bucketId = ixfileHandle.getLinearHash()->lookforBucket(attribute, key);

	int rc = ixfileHandle.getLinearHash()->insertEntry(bucketId, attribute, entry);
	assert(rc == SUCCESS);

//	if(ixfileHandle.getLinearHash().isSplitTriggerred()){
//		cout<<"##splitTriggerred..."<<endl;
//		ixfileHandle.getLinearHash().split(attribute);
//	}else{
//
//	}
/*	int level = ixfileHandle.getLinearHash().getMetaData()->getLevel();
	int bucketId = ixfileHandle.getLinearHash().hash(attribute, key, level);
	int rc;
	if(bucketId >=ixfileHandle.getLinearHash().getMetaData()->getNext())
	{
		rc = ixfileHandle.getLinearHash().insertEntry(bucketId, attribute, entry);
		assert(rc == SUCCESS);
	}else{
		//calculate the
		bucketId = ixfileHandle.getLinearHash().hash(attribute, key, level+1);
		rc = ixfileHandle.getLinearHash().insertEntry(bucketId, attribute, entry);
		assert(rc == SUCCESS);
	}
*/
	return SUCCESS;

}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	unsigned bucketId = ixfileHandle.getLinearHash()->lookforBucket(attribute, key);
	int rc = ixfileHandle.getLinearHash()->deleteEntry(bucketId, attribute, key, rid);
	assert(rc == SUCCESS);
	return SUCCESS;
}

unsigned IndexManager::hash(const Attribute &attribute, const void *key)
{
	return 0;
}

RC IndexManager::printIndexEntriesInAPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const unsigned &primaryPageNumber) 
{
	cout<<"Prime";
	ixfileHandle.getLinearHash()->getBucket(primaryPageNumber)->print(attribute);
	int primeEntryNum = ixfileHandle.getLinearHash()->getBucket(primaryPageNumber)->getLast();

	cout<<"last = "<<primeEntryNum<<endl;

	int overflowEntryNum = 0;
	if(ixfileHandle.getLinearHash()->getBucket(primaryPageNumber)->isOverFlow())
	{
		vector<Bucket*> buckets = ixfileHandle.getLinearHash()->getOverFlowPages()->getValue(primaryPageNumber);

		for(int i=0; i < buckets.size(); i++)
		{
			cout<<"OverFlow";
			buckets[i]->print(attribute);
			overflowEntryNum += buckets[i]->getLast();
		}
	}
	cout<<"Number of total entries in the page (+ overflow pages) :"<<(primeEntryNum + overflowEntryNum)<<endl;

	return SUCCESS;
}

RC IndexManager::getNumberOfPrimaryPages(IXFileHandle &ixfileHandle, unsigned &numberOfPrimaryPages) 
{
	numberOfPrimaryPages = ixfileHandle.getLinearHash()->getPrimeBucketNumber();
	return SUCCESS;
}

RC IndexManager::getNumberOfAllPages(IXFileHandle &ixfileHandle, unsigned &numberOfAllPages) 
{
	numberOfAllPages = ixfileHandle.getLinearHash()->getPrimeBucketNumber();
	cout<<"primery page number: "<<numberOfAllPages<<endl;
	vector<int>* keys = ixfileHandle.getLinearHash()->getOverFlowPages()->getKeySet();
	for(int i=0;i< keys->size();i++)
	{
		vector<Bucket*> buckets = ixfileHandle.getLinearHash()->getOverFlowPages()->getValue(keys->at(i));
		numberOfAllPages += buckets.size();
	}

	return SUCCESS;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
    const Attribute &attribute,
    const void      *lowKey,
    const void      *highKey,
    bool			lowKeyInclusive,
    bool        	highKeyInclusive,
    IX_ScanIterator &ix_ScanIterator)
{
	int rc;
	bool equality = false;
	cout<<"begin to IndexManager::scanning.."<<endl;
	if (lowKey != NULL && highKey != NULL && lowKeyInclusive == true && highKeyInclusive == true) {
		switch (attribute.type) {
		case 0: {
			int low, high;
			memcpy(&low, lowKey, sizeof(int));
			memcpy(&high, highKey, sizeof(int));
			if (low == high) {
				cout<<"equality!"<<endl;
				equality = true;
			}
			break;
		}
		case 1: {
			float low, high;
			memcpy(&low, lowKey, sizeof(float));
			memcpy(&high, highKey, sizeof(float));
			if (low == high) {
				cout<<"equality!"<<endl;
				equality = true;
			}
			break;
		}
		case 2: {
			if (strcmp((char *) lowKey, (char *) highKey) == 0) {
				cout<<"equality!"<<endl;
				equality = true;
			}
			break;
		}
		default: {
			break;
		}
		}
	}

	if(equality){
		rc = ixfileHandle.getLinearHash()->equalityScan(attribute, lowKey, ix_ScanIterator);
		assert(rc==0);
	}else{
		cout<<"range !"<<endl;
		rc = ixfileHandle.getLinearHash()->rangeScan(attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, ix_ScanIterator);
		assert(rc==0);
	}

	return 0;
}

IX_ScanIterator::IX_ScanIterator()
{
	pos = 0;
	eof = 0;
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::addEntry(const Entry entry)
{

	cout<<"No. "<<eof<<"[key=";
	switch(type){
	case 0:{
		int key1 = entry.key.key1;
		cout<<key1;
		break;}
	case 1:{
		float key2 = entry.key.key2;
		cout<<key2;

		break;}
	case 2:{
		int len;
		memcpy(&len, entry.key.key3, sizeof(int));
		cout<<((char *)entry.key.key3 + len);
		break;}
	default:
		break;
	}
	cout<<"/"<<entry.rid.pageNum<<","<<entry.rid.slotNum<<"]"<<endl;

	entries.push_back(entry);
	eof ++;
	return SUCCESS;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	if(pos == eof)
	{
		return End_Of_File;
	}

	rid.pageNum = entries[pos].rid.pageNum;
	rid.slotNum = entries[pos].rid.slotNum;

	switch(type)
	{
	case 0:
		memcpy((char *)key, &entries[pos].key.key1, sizeof(int));
//		key = entries[pos].key.key1;
		break;
	case 1:
		memcpy((char *)key, &entries[pos].key.key2, sizeof(float));
		break;
	case 2:
		memcpy((char *)key, entries[pos].key.key3, sizeof(int));
		break;
	}
	pos++;
	return SUCCESS;
}

RC IX_ScanIterator::close()
{
	entries.clear();
	pos = -1;
	eof = -1;
	return SUCCESS;
}


IXFileHandle::IXFileHandle()
{
	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter = 0;
	pageMaxNum = 0;
	linearHashing = new LinearHash();
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::setIndexHandle(FILE *p)
{
	pIndex = p;
	return 0;
}

RC IXFileHandle::setMedatDataHandle(FILE *p)
{
	pMetaData = p;
	return 0;
}

RC IXFileHandle::closeHandle()
{
	fclose(pIndex);
	fclose(pMetaData);
	return SUCCESS;
}

RC IXFileHandle::readMetaData()
{
	int initial_capacity, numberOfPages, level, type;
	unsigned next;
	int offset = 0;

	fseek(pMetaData, offset, SEEK_SET);
	fread(&initial_capacity, sizeof(unsigned), 1, pMetaData);
	offset += sizeof(unsigned);

	cout<<"initial_capacity = "<<initial_capacity<<endl;

	fseek(pMetaData, offset, SEEK_SET);
	fread(&numberOfPages, sizeof(unsigned), 1, pMetaData);
	offset += sizeof(unsigned);

	cout<<"numberOfPages = "<<numberOfPages<<endl;

	fseek(pMetaData, offset, SEEK_SET);
	fread(&next, sizeof(unsigned), 1, pMetaData);
	offset += sizeof(unsigned);

	cout<<"next = "<<next<<endl;

	fseek(pMetaData, offset, SEEK_SET);
	fread(&level, sizeof(int), 1, pMetaData);
	offset += sizeof(int);

	cout<<"level = "<<level<<endl;

	fseek(pMetaData, offset, SEEK_SET);
	fread(&type, sizeof(int), 1, pMetaData);

	cout<<"type = "<<type<<endl;

	MetaData *mData  = new MetaData(initial_capacity, next, level, numberOfPages);
	mData->setType(type);
	linearHashing->setMetaData(mData);

	return SUCCESS;

}

RC IXFileHandle::readFlowPages()
{
	int offset = sizeof(unsigned) + 5 * sizeof(int);
	fseek(pMetaData, offset, SEEK_SET);

	int numberOfPagesEachBucket = linearHashing->getMetaData()->getPageNum();
	int type = linearHashing->getMetaData()->getType();

	OverFLowPages *overFlowPages = new OverFLowPages();

	if(type==-1){
		linearHashing->setOverFlowPages(overFlowPages);
		return SUCCESS;
	}

	int primePageId;
	int bucketSize, last, entryMaxNumber, keysSize;

	//read the hash_map size
	fread(&keysSize, sizeof(int), 1, pMetaData);
	offset += sizeof(int);

	cout<<"flow key size"<<keysSize<<endl;

	for(int i=0;i < keysSize;i++)
	{
		//read PimePageId
		fseek(pMetaData, offset, SEEK_SET);
		fread(&primePageId, sizeof(int), 1, pMetaData);
		offset += sizeof(int);
		cout<<"primePageId:"<<primePageId;
		//read #buckets
		fseek(pMetaData, offset, SEEK_SET);
		fread(&bucketSize, sizeof(int), 1, pMetaData);
		offset += sizeof(int);

		vector<Bucket*> p;
		for(int j=0; j<bucketSize; j++)
		{
			//read information of each bucket
			fseek(pMetaData, offset, SEEK_SET);
			fread(&last, sizeof(int), 1, pMetaData);
			offset += sizeof(int);

			//Bucket *bucket = new Bucket(primePageId, numberOfPagesEachBucket);
			Bucket *bucket = new Bucket(primePageId);
			for(int k=0; k<last; k++)
			{
				//read information of each Entry
				Entry entry;
				cout<<"[key=";
				fseek(pMetaData, offset, SEEK_SET);
				switch(type){
				case 0:
					fread(&entry.key.key1, sizeof(int), 1, pMetaData);
					cout<<entry.key.key1;
					offset += sizeof(int);
					break;
				case 1:
					fread(&entry.key.key2, sizeof(float), 1, pMetaData);
					cout<<entry.key.key2;
					offset += sizeof(float);
					break;
				case 2:
					int len;
					fread(&len, sizeof(int), 1, pIndex);
					fread(entry.key.key3, len + 4, 1, pMetaData);
					cout<<((char *)entry.key.key3 + len);
					offset += (len +4);
					break;
				default:
					break;
				}
				fseek(pMetaData, offset, SEEK_SET);
				fread(&entry.rid.pageNum, sizeof(unsigned), 1, pMetaData);
				offset += sizeof(unsigned);
				cout<<"/"<<entry.rid.pageNum<<",";

				fseek(pMetaData, offset, SEEK_SET);
				fread(&entry.rid.slotNum, sizeof(unsigned), 1, pMetaData);
				offset += sizeof(unsigned);
				cout<<entry.rid.slotNum<<"]";

				bucket->addEntry(entry, type);
			}
			cout<<endl;
			p.push_back(bucket);
		}
		overFlowPages->buildOverFlowHashMap(primePageId, p);
	}

	linearHashing->setOverFlowPages(overFlowPages);

	return SUCCESS;
}

RC IXFileHandle::readPrimePages()
{
	int offset = 0;
	int bucketId, last;
	int primeBucketNumber = linearHashing->getMetaData()->getPageNum();
//	int bucketCapacity = linearHashing.getMetaData()->getBucketCapacity();
	int type = linearHashing->getMetaData()->getType();

	if(type==-1){
		for(int i=0; i < linearHashing->getMetaData()->getInitialCapacity(); i++){
			Bucket *bucket = new Bucket(i);
			linearHashing->appendBucket(bucket);
		}
		return SUCCESS;
	}

	//write prime BucketNumber
//	fseek(pIndex, offset, SEEK_SET);
//	fread(&primeBucketNumber, sizeof(int), 1, pIndex);
//	offset += sizeof(int);

//	linearHashing.

	for(bucketId=0; bucketId<primeBucketNumber;bucketId++)
	{
		//write each bucket size
		Bucket *p = new Bucket(bucketId);// = new Bucket(bucketId, bucketCapacity);//linearHashing.getBucket(bucketId);
//		p.setId(bucketId);
//		p.setCapacity(bucketCapacity);
		//last = p.getBucketSize();
		fseek(pIndex, offset, SEEK_SET);
		fread(&last, sizeof(int), 1, pIndex);
		offset += sizeof(int);
		cout<<"bucketId:"<<bucketId;

		//write whether it's overflow
		bool overflow;// = p.isOverFlow();
		fseek(pIndex, offset, SEEK_SET);
		fread(&overflow, sizeof(bool), 1, pIndex);
		offset += sizeof(bool);

		if(overflow){
			p->setOverFlow();
		}

		for(int entryId =0; entryId<last; entryId++)
		{
			//write each entry of bucket
			Entry entry;
			cout<<"[key=";
			fseek(pIndex, offset, SEEK_SET);
			switch(type)
			{
			case 0:
				int key1;
				fread(&key1, sizeof(int), 1, pIndex);
				entry.key.key1 = key1;
				cout<<key1;
				offset += sizeof(int);
				break;
			case 1:
				float key2;
				fread(&key2, sizeof(float), 1, pIndex);
				cout<<key2;
				entry.key.key2 = key2;
				offset += sizeof(float);
				break;
			case 2:
				int len;
				fread(&len, sizeof(int), 1, pIndex);
				fread(entry.key.key3, len + 4, 1, pIndex);
				cout<<((char *)entry.key.key3 + len);
				offset += (4 + len);
				break;
			default:
				printf("type undefined!\n");
				break;
			}

			fseek(pIndex, offset, SEEK_SET);
			unsigned pageNum;
			fread(&pageNum, sizeof(unsigned), 1, pIndex);
			offset += sizeof(unsigned);
			cout<<"/"<<pageNum<<",";

			fseek(pIndex, offset, SEEK_SET);
			unsigned slotNum;
			fread(&slotNum, sizeof(unsigned), 1, pIndex);
			offset += sizeof(unsigned);
			cout<<slotNum<<"]";

			entry.rid.pageNum = pageNum;
			entry.rid.slotNum = slotNum;

			p->addEntry(entry, type);
		}
		cout<<endl;
		linearHashing->appendBucket(p);
	}

	return SUCCESS;
}


RC IXFileHandle::writeMetaData()
{
	MetaData *mData = linearHashing->getMetaData();

	int initial_capacity = mData->getInitialCapacity();
	unsigned numberOfPages = linearHashing->getPrimeBucketNumber();
	unsigned next = mData->getNext();
	int level = mData->getLevel();
	int type = mData->getType();

	int offset = 0;

	fseek(pMetaData, offset, SEEK_SET);
	fwrite(&initial_capacity, sizeof(int), 1, pMetaData);
	offset += sizeof(int);
	cout<<"initial_capacity = "<<initial_capacity<<endl;

	fseek(pMetaData, offset, SEEK_SET);
	fwrite(&numberOfPages, sizeof(unsigned), 1, pMetaData);
	offset += sizeof(unsigned);
	cout<<"numberOfPages = "<<numberOfPages<<endl;

	fseek(pMetaData, offset, SEEK_SET);
	fwrite(&next, sizeof(unsigned), 1, pMetaData);
	offset += sizeof(unsigned);
	cout<<"next = "<<next<<endl;

	fseek(pMetaData, offset, SEEK_SET);
	fwrite(&level, sizeof(int), 1, pMetaData);
	offset += sizeof(int);
	cout<<"level = "<<level<<endl;

	fseek(pMetaData, offset, SEEK_SET);
	fwrite(&type, sizeof(int), 1, pMetaData);
	cout<<"type = "<<type<<endl;

	return SUCCESS;
}


RC IXFileHandle::writeFlowPages()
{
	int offset = sizeof(unsigned) + 5 * sizeof(int);
	fseek(pMetaData, offset, SEEK_SET);

	OverFLowPages *overFlowPages = linearHashing->getOverFlowPages();
	vector<int>* keys = overFlowPages->getKeySet();

	int type = linearHashing->getMetaData()->getType();

	int primePageId;
	int bucketId, last, keysSize;
	//write the size of hash_map
	keysSize = keys->size();

	cout<<"write flow pages: key size:"<<keysSize<<endl;

	fwrite(&keysSize, sizeof(int), 1, pMetaData);
	offset += sizeof(int);

	for(int i=0; i < keys->size();i++)
	{
		//write PimePageId
		primePageId = keys->at(i);
		fseek(pMetaData, offset, SEEK_SET);
		fwrite(&primePageId, sizeof(int), 1, pMetaData);
		offset += sizeof(int);
		cout<<"primery Id:"<<primePageId;
		//write #buckets
		vector<Bucket*> buckets = overFlowPages->getValue(primePageId);
		int bucketSize = buckets.size();
		fseek(pMetaData, offset, SEEK_SET);
		fwrite(&bucketSize, sizeof(int), 1, pMetaData);
		offset += sizeof(int);

		for(int j=0; j < bucketSize; j++)
		{
			//write information of each bucket
//			bucketId = buckets.at(j)->getBucketId();
//			fwrite(&bucketId, sizeof(int), 1, pMetaData);
//			offset += sizeof(int);

			last = buckets.at(j)->getBucketSize();
			fseek(pMetaData, offset, SEEK_SET);
			fwrite(&last, sizeof(int), 1, pMetaData);
			offset += sizeof(int);

//			entryMaxNumber = buckets.at(j)->getBucketCapacity();
//			fwrite(&entryMaxNumber, sizeof(int), 1, pMetaData);
//			offset += sizeof(int);

			for(int k=0; k<last; k++)
			{
				Entry entry =  buckets.at(j)->getEntry(k);
				//write key
				cout<<"[key=";
				fseek(pMetaData, offset, SEEK_SET);
				switch(type)
				{
				case 0:
				{
					int key1 = entry.key.key1;
					cout<<key1;
//					memcpy(&key1, (char *)entry.key, 4);
					fwrite(&key1, 4, 1, pMetaData);
					offset +=4;
					break;
				}
				case 1:
				{
					float key2 = entry.key.key2;
					cout<<key2;
//					memcpy(&key2, (char *)entry.key, 4);
					fwrite(&key2, 4, 1, pMetaData);
					offset +=4;
					break;
				}
				case 2:
				{
					int len;
					memcpy(&len, (char *)entry.key.key3, 4);
					fwrite(entry.key.key3, 4 + len, 1,  pMetaData);
					cout<<((char *)entry.key.key3 + len);
					offset += (4 + len);
					break;
				}
				default:
				{	printf("type undefined\n");
					break;
				}
				}
				//write rid
				fseek(pMetaData, offset, SEEK_SET);
				fwrite(&entry.rid.pageNum, sizeof(unsigned), 1, pMetaData);
				offset += sizeof(unsigned);
				cout<<"/"<<entry.rid.pageNum<<",";

				fseek(pMetaData, offset, SEEK_SET);
				fwrite(&entry.rid.slotNum, sizeof(unsigned), 1, pMetaData);
				offset += sizeof(unsigned);
				cout<<entry.rid.slotNum<<"]";
			}
			cout<<endl;
		}
	}

	return SUCCESS;
}

RC IXFileHandle::writePrimePages()
{
	int offset = 0;
	int bucketId, last;
	int primeBucketNumber = linearHashing->getPrimeBucketNumber();
	int type = linearHashing->getMetaData()->getType();

	//write prime BucketNumber
//	fseek(pIndex, offset, SEEK_SET);
//	fwrite(&primeBucketNumber, sizeof(int), 1, pIndex);
//	offset += sizeof(int);

	for(bucketId=0; bucketId<primeBucketNumber;bucketId++)
	{
		//write each bucket size
		cout<<"buckd id="<<bucketId;
		Bucket *p = linearHashing->getBucket(bucketId);
		last = p->getBucketSize();
		fseek(pIndex, offset, SEEK_SET);
		fwrite(&last, sizeof(int), 1, pIndex);
		offset += sizeof(int);

		//write whether it's overflow
		bool overflow = p->isOverFlow();
		fseek(pIndex, offset, SEEK_SET);
		fwrite(&overflow, sizeof(bool), 1, pIndex);
		offset += sizeof(bool);

		for(int entryId =0; entryId<last; entryId++)
		{
			//write each entry of bucket
			cout<<"[key=";
			Entry entry = p->getEntry(entryId);
			fseek(pIndex, offset, SEEK_SET);
			switch(type)
			{
			case 0:
			{
				int key1 = entry.key.key1;
				cout<<key1;
//				memcpy(&key1, (char *)entry.key, 4);
				fwrite(&key1, 4, 1, pIndex);
				offset +=4;
				break;
			}
			case 1:
			{
				float key2 = entry.key.key2;
				cout<<key2;
//				memcpy(&key2, (char *)entry.key, 4);
				fwrite(&key2, 4, 1, pIndex);
				offset +=4;
				break;
			}
			case 2:
			{	int len;
				memcpy(&len, (char *)entry.key.key3, 4);
				fwrite(entry.key.key3, 4 + len, 1,  pIndex);
				cout<<((char *)entry.key.key3+len);
				offset += (4 + len);
				break;
			}
			default:
				{	printf("type undefined!\n");
					break;
				}
			}
			//write rid
			fseek(pIndex, offset, SEEK_SET);
			//fwrite(&entry.rid, sizeof(RID), 1, pIndex);
			//offset += sizeof(RID);
			fwrite(&entry.rid.pageNum, sizeof(unsigned), 1, pIndex);
			cout<<"/"<<entry.rid.pageNum;
			offset += sizeof(unsigned);

			fseek(pIndex, offset, SEEK_SET);
			fwrite(&entry.rid.slotNum, sizeof(unsigned), 1, pIndex);
			cout<<","<<entry.rid.slotNum<<"]";
			offset += sizeof(unsigned);
		}
		cout<<endl;
	}

	return SUCCESS;
}



LinearHash *IXFileHandle::getLinearHash()
{
	return linearHashing;
}

RC IXFileHandle::readPage(PageNum pageNum, void *data)
{
	cout<<"IXFileHandle: readPage"<<endl;

	//printf("......entering readPage()......\n");
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
		//printf("pagenumber: %u, maxPageNUm : %i\n",pageNum, pageMaxNum);
	}

	fseek(pIndex, pageNum * PAGE_SIZE, SEEK_SET);
	fread(data, 1, PAGE_SIZE, pIndex);

	readPageCounter++;

    return 0;
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data)
{
	if(pageMaxNum == -1){
		return -1001;
	}
	if(pageMaxNum < pageNum)
	{
		return -1000;
	}else{
		//printf("pagenumber: %u, maxPageNUm : %i\n",pageNum, pageMaxNum);
	}

	fseek (pIndex, pageNum * PAGE_SIZE, SEEK_SET);
	fwrite(data, 1, PAGE_SIZE, pIndex);

	writePageCounter++;
    return 0;
}


RC IXFileHandle::appendPage(const void *data)
{
	pageMaxNum++;
	fseek(pIndex, pageMaxNum * PAGE_SIZE, SEEK_SET);
	int count = fwrite(data, 1, PAGE_SIZE, pIndex);
	printf("new page appended! no.%d, bytes: %d \n", pageMaxNum, count);

//	int bufferId = lookforBuffer();
//	memcpy(pageBuffer[bufferId], (char *)data, PAGE_SIZE);
//	pageToBuffer[pageMaxNum] = bufferId;
//	bufferToPage[bufferId] = pageMaxNum;

	appendPageCounter++;
    return 0;
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount = readPageCounter;
	writePageCount = writePageCounter;
	appendPageCount = appendPageCounter;
	return SUCCESS;
}

void IX_PrintError (RC rc)
{
	string msg;
	switch(rc)
	{
	case 0:
		msg = "SUCCESS";
		break;
	case -127:
		msg = "INDEX_EXISTS";
		break;
	case -129:
		msg = "FILE_OPEN_FAIL";
		break;
	case -131:
		msg = "METADATA_EXISTS";
		break;
	case -133:
		msg = "PRIMEPAGES_FULL";
		break;
	case -135:
		msg = "BUCKETS_FULL";
		break;
	case -137:
		msg = "PAGEID_EXCEEDS";
		break;
	case -139:
		msg = "ENTRY_NOT_EXIST";
		break;
	case -141:
		msg = "KEY_NOT_FOUNT";
		break;
	case -143:
		msg = "End_Of_File";
		break;
	default:
		msg = "error code not found";
		break;
	}

	printf("rc = %s\n", msg.c_str());
}
