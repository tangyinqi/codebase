
#include "rm.h"


RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
	//creat tables' table
/*	int rc = RecordBasedFileManager::instance()->createFile(TABLE);
	assert(rc==0);

	//create the catalog for each table
	string catalogName = TABLE.append(string(CATALOG));
	rc = RecordBasedFileManager::instance()->createFile(catalogName);
	assert(rc==0);

	vector<Attribute> tableNameAttributes;
	Attribute attr;
	attr.name = "table_name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)10;
	tableNameAttributes.push_back(attr);

	attr.name = "file_name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)10;
	tableNameAttributes.push_back(attr);

	attributes[TABLE] = tableNameAttributes;

	FileHandle filehandle_catalog;
	filehandles_catalog[TABLE] = filehandle_catalog;

	rc = RecordBasedFileManager::instance()->openFile(catalogName, filehandle_catalog);
	assert(rc==0);

	for (int i = 0; i < tableNameAttributes.size(); i++) {
		vector<Attribute> recordAttribute;
		Attribute attr;
		int length = 4; //size of name
		length += attributes[i].length(); //name

		length += 4; //type int
		length += 4; //position int

		cout<<"TABLE.length:"<<TABLE.length<<endl;

		void * data = (void *) malloc(length);
		unsigned len = (AttrLength) attributes[i].length();
		memcpy((char *) data, &len, sizeof(int));
		memcpy((char *) data + 4, (char *) attributes[i].c_str(), attributes[i].length());
		attr.name = attributes[i];
		attr.type = TypeVarChar;
		attr.length = (AttrLength) attributes[i].length();
		recordAttribute.push_back(attr);

		memcpy((char *) data + 4 + attributes[i].length(), TABLE, TABLE.length);
		attr.name = "rel_Name";
		attr.type = TypeVarChar;
		attr.length = (AttrLength) .length();
		recordAttribute.push_back(attr);

		memcpy((char *) data + 4 + attributes[i].length(), &attrs[i].type, sizeof(int));
		attr.name = "TYPE";
		attr.type = TypeInt;
		attr.length = (AttrLength) 4;
		recordAttribute.push_back(attr);

		memcpy((char *) data + 8 + attrs[i].name.length(), &i, sizeof(int));
		attr.name = "Position";
		attr.type = TypeInt;
		attr.length = (AttrLength) 4;
		recordAttribute.push_back(attr);
		RID rid;

		rc = RecordBasedFileManager::instance()->insertRecord(filehandle_catalog, recordAttribute, data, rid);
		assert(rc==0);

		//if(rc!=0)return rc;
		recordAttribute.clear();
		free(data);
	}

	tableHandle.savePageInfo();
	*/
}

RelationManager::~RelationManager()
{
	filehandles.clear();
	filehandles_catalog.clear();
	attributes.clear();

}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	//create file tableName
	int rc = RecordBasedFileManager::instance()->createFile(tableName);
	assert(rc==0);
	if(rc != 0)return rc;

	//create the catalog for each table
	string catalogName = string(tableName).append(string(CATALOG));
	rc = RecordBasedFileManager::instance()->createFile(catalogName);
	assert(rc==0);
	if(rc != 0)return rc;

	//for each attribute: attrs, assemble it as a record with format: name | type| length| postion
	// e.g: attribute.name = type, attribute.length = ?, attribute.type = string
    //insert the

	vector<Attribute> tableNameAttributes;
	for(int j=0; j<attrs.size(); j++)
	{
		tableNameAttributes.push_back(attrs[j]);
	}
	attributes[tableName] = tableNameAttributes;

	FileHandle filehandle_catalog;
	filehandles_catalog[tableName] = filehandle_catalog;

	rc = RecordBasedFileManager::instance()->openFile(catalogName, filehandle_catalog);
	assert(rc==0);
	if(rc != 0)return rc;

	for(int i=0;i<attrs.size();i++)
	{
		vector<Attribute> recordAttribute;
		Attribute attr;
		int length = 4;//size of name
		length += attrs[i].name.length(); //name
		length += 4; //type int
		length += 4; //position int
		void * data = (void *)malloc(length);
		unsigned len = (AttrLength)attrs[i].name.length();
		memcpy((char *)data, &len, sizeof(int));
		memcpy((char *)data + 4, (char *) attrs[i].name.c_str(), attrs[i].name.length());
		attr.name = "attr_name";
		attr.type = TypeVarChar;
		attr.length = (AttrLength)attrs[i].name.length();
		recordAttribute.push_back(attr);

		memcpy((char *)data + 4 + attrs[i].name.length(), &attrs[i].type, sizeof(int));
		attr.name = "TYPE";
		attr.type = TypeInt;
		attr.length = (AttrLength)4;
		recordAttribute.push_back(attr);

		memcpy((char *)data + 8 + attrs[i].name.length(), &i, sizeof(int));
		attr.name = "Position";
		attr.type = TypeInt;
		attr.length = (AttrLength)4;
		recordAttribute.push_back(attr);
		RID rid;
		rc = RecordBasedFileManager::instance()->insertRecord(filehandle_catalog, recordAttribute, data, rid);
		assert(rc==0);

		//if(rc!=0)return rc;
		recordAttribute.clear();
		free(data);
	}

	rc = filehandle_catalog.savePageInfo();
	//rc = RecordBasedFileManager::instance()->closeFile(filehandle_catalog);
	assert(rc == 0);
	if(rc != 0)return rc;

//	insert information to the "Tables."
//	rc = tableHandle.readPageInfo();
//	assert(rc==0);
//	rc = RecordBasedFileManager::instance()->insertRecord(tableHandle, recordAttribute, data, rid);
//	assert(rc==0);
//	rc = tableHandle.savePageInfo();
//	assert(rc==0);


	return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
	int rc = deleteTuples(tableName);
	assert(rc == 0);

	if (FileExists(tableName.c_str())) {
		FileHandle filehandle;
		if(filehandles.find(tableName) != filehandles.end()){
			filehandle = filehandles[tableName];
			printf("#deleteTable(): filehandles found in table\n");
			filehandle.readPageInfo();
		}else{
			rc = RecordBasedFileManager::instance()->openFile(tableName, filehandle);
			assert(rc == 0);
		}

		rc = RecordBasedFileManager::instance()->closeFile(filehandle);
		assert(rc==0);

		rc = RecordBasedFileManager::instance()->destroyFile(tableName);
		assert(rc==0);

		//if the filehandle is closed, it must be removed from the map
		filehandles.erase(tableName);
	}


	//delete the catalog of table
	string tableNameCatalog = string(tableName).append(CATALOG);
	if (FileExists(tableNameCatalog.c_str())) {
		FileHandle filehandle_catalog;
		if (filehandles_catalog.find(tableName) != filehandles_catalog.end()) {
			filehandle_catalog = filehandles_catalog[tableName];
			printf("#deleteTable(): filehandle_catalog found in table\n");
			filehandle_catalog.readPageInfo();
		} else {
			rc = RecordBasedFileManager::instance()->openFile(tableNameCatalog, filehandle_catalog);
			assert(rc == 0);
		}

		rc = RecordBasedFileManager::instance()->deleteRecords(filehandle_catalog);
		assert(rc == 0);

		rc = RecordBasedFileManager::instance()->closeFile(filehandle_catalog);
		assert(rc==0);

		rc = RecordBasedFileManager::instance()->destroyFile(tableNameCatalog);
		assert(rc==0);


		//if the filehandle is closed, it must be removed from the map
		filehandles_catalog.erase(tableName);

	}

	//remove the cache of relation attributes
	attributes.erase(tableName);

    return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	if(attributes.find(tableName) != attributes.end()){
		attrs = attributes[tableName];
		return 0;
	}

	if(!FileExists(tableName.c_str())){
		printf("%s doesn't exist!", tableName.c_str());
		return -1;
	}

	string catalogName = string(tableName).append(CATALOG);
	if(!FileExists(catalogName.c_str())){
		printf("%s doesn't exist!", catalogName.c_str());
		return -1;
	}

	FileHandle filehandle_catalog;
	int rc;

	if(filehandles_catalog.find(tableName) != filehandles_catalog.end()){
		filehandle_catalog = filehandles_catalog[tableName];
		printf("#getAttributes():filehandle found in table!\n");
		filehandle_catalog.readPageInfo();
	}else{
		rc = RecordBasedFileManager::instance()->openFile(catalogName, filehandle_catalog);
		assert(rc==0);
		filehandles_catalog[tableName] = filehandle_catalog;
	}

	for(unsigned pNum = 0; pNum<filehandle_catalog.getNumberOfPages(); pNum++)
	{
		for(unsigned sNum=0; sNum<=filehandle_catalog.getLastSlotNum(pNum); sNum++)
		{
			// get rid if the slot is not deleted
			if(!filehandle_catalog.isSlotDeleted(pNum, sNum)){
				RID rid;
				// get
				if(filehandle_catalog.isSlotTombStone(pNum, sNum)){
					filehandle_catalog.getSlotTombStoneRID(pNum, sNum, rid);
				}else{
					rid.pageNum = pNum;
					rid.slotNum = sNum;
				}
				int datalen = filehandle_catalog.getLength(rid.pageNum, rid.slotNum);
				void *data = (void *)malloc(datalen);
				vector<Attribute> attrsnull;
				rc = RecordBasedFileManager::instance()->readRecord(filehandle_catalog, attrsnull, rid, data);
				assert(rc == 0);

				int namelen;
				memcpy(&namelen, data, 4);

				char *name = (char *)malloc(namelen);
				memcpy(name, (char *)data + 4, namelen);
				printf("attribute name: %s\n", name);

				int type;
				memcpy(&type, (char *)data + 4 + namelen, 4);
				Attribute attr;
				attr.name = name;
				attr.type = (AttrType)type;

				switch(type){
				case 0:
				case 1:
					attr.length = (AttrLength)4;
					break;
				case 2:
					attr.length = (AttrLength)namelen;
					break;
				}
				attrs.push_back(attr);
				free(data);
				free(name);
			}
		}
	}
	attributes[tableName] = attrs;

    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	if(!FileExists(tableName.c_str())){
		printf("%s doesn't exist!", tableName.c_str());
		return -1;
	}

	vector<Attribute> attri;
	int rc;
	if(attributes.find(tableName) != attributes.end()){
		attri = attributes[tableName];
	}else{
		rc = getAttributes(tableName, attri);
		assert(rc==0);
		attributes[tableName] = attri;
	}

	FileHandle filehandle;
	if (filehandles.find(tableName) != filehandles.end()) {
		filehandle = filehandles[tableName];

//		printf("#insertTuple():filehandle found in table!\n");
		filehandle.readPageInfo();
	} else {
		rc = RecordBasedFileManager::instance()->openFile(tableName, filehandle);
		assert(rc == 0);
		filehandles[tableName] = filehandle;
//		printf("#insertTuple():filehandle not found in table, new open file!\n");
	}

	rc = RecordBasedFileManager::instance()->insertRecord(filehandle, attri, data, rid);
	assert(rc == 0);
	//rc = RecordBasedFileManager::instance()->closeFile(filehandle);
	//assert(rc == 0);
	rc = filehandle.savePageInfo();
	assert(rc == 0);
    return 0;
}

RC RelationManager::deleteTuples(const string &tableName)
{
	if(!FileExists(tableName.c_str())) return -1;

	FileHandle filehandle;
	int rc;
	if(filehandles.find(tableName) == filehandles.end())
	{
		rc = RecordBasedFileManager::instance()->openFile(tableName, filehandle);
		assert(rc == 0);
		filehandles[tableName] = filehandle;
	}else{
		filehandle = filehandles[tableName];
		printf("#deleteTuples():filehandle found in table!\n");
		filehandle.readPageInfo();
	}

	rc = RecordBasedFileManager::instance()->deleteRecords(filehandle);
	assert(rc == 0);
    return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	if(!FileExists(tableName.c_str())) return -1;

	int rc;
	FileHandle filehandle;
	if(filehandles.find(tableName) == filehandles.end())
	{
		rc = RecordBasedFileManager::instance()->openFile(tableName, filehandle);
		assert(rc == 0);
		filehandles[tableName] = filehandle;

	}else{
		filehandle = filehandles[tableName];
		printf("#deleteTuple():filehandle found in table!\n");
		filehandle.readPageInfo();
	}

	vector<Attribute> attri;
	if(attributes.find(tableName) != attributes.end()){
		attri = attributes[tableName];
	}else{
		rc = getAttributes(tableName, attri);
		assert(rc==0);
		attributes[tableName] = attri;
	}

	rc = RecordBasedFileManager::instance()->deleteRecord(filehandle, attri, rid);
	assert(rc == 0);
	rc = filehandle.savePageInfo();
	assert(rc == 0);
    return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	if(!FileExists(tableName.c_str())){
		printf("%s doesn't exist!", tableName.c_str());
		return -1;
	}

	vector<Attribute> attri;
	int rc;
	if(attributes.find(tableName) != attributes.end()){
		attri = attributes[tableName];
	}else{
		rc = getAttributes(tableName, attri);
		assert(rc==0);
		attributes[tableName] = attri;
	}

	FileHandle filehandle;
	if (filehandles.find(tableName) != filehandles.end()) {
		filehandle = filehandles[tableName];
		printf("#updateTuple():filehandle found in table!\n");
		filehandle.readPageInfo();
	} else {
		rc = RecordBasedFileManager::instance()->openFile(tableName, filehandle);
		assert(rc == 0);
		filehandles[tableName] = filehandle;
	}

	rc = RecordBasedFileManager::instance()->updateRecord(filehandle, attri, data, rid);
	assert(rc == 0);
	rc = filehandle.savePageInfo();
	assert(rc == 0);
    return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	if(!FileExists(tableName.c_str())){
		printf("%s doesn't exist!", tableName.c_str());
		return -1;
	}

	vector<Attribute> attri;
	int rc;
	if(attributes.find(tableName) != attributes.end()){
		attri = attributes[tableName];
	}else{
		rc = getAttributes(tableName, attri);
		assert(rc==0);
		attributes[tableName] = attri;
	}

	FileHandle filehandle;
	if (filehandles.find(tableName) != filehandles.end()) {
		filehandle = filehandles[tableName];
//		printf("#readTuple():filehandle found in table!\n");
		filehandle.readPageInfo();
	} else {
		rc = RecordBasedFileManager::instance()->openFile(tableName, filehandle);
		assert(rc == 0);
		filehandles[tableName] = filehandle;
//		printf("#readTuple():filehandle not found in table! new open File!\n");
	}

	rc = RecordBasedFileManager::instance()->readRecord(filehandle, attri, rid, data);
	//assert(rc == 0);

    return rc;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	if(!FileExists(tableName.c_str())){
		printf("%s doesn't exist!", tableName.c_str());
		return -1;
	}

	vector<Attribute> attri;
	int rc;
	if(attributes.find(tableName) != attributes.end()){
		attri = attributes[tableName];
	}else{
		rc = getAttributes(tableName, attri);
		assert(rc==0);
		attributes[tableName] = attri;
	}

	FileHandle filehandle;
	if (filehandles.find(tableName) != filehandles.end()) {
		filehandle = filehandles[tableName];
		filehandle.readPageInfo();
	} else {
		rc = RecordBasedFileManager::instance()->openFile(tableName, filehandle);
		assert(rc == 0);
		filehandles[tableName] = filehandle;
	}

	rc = RecordBasedFileManager::instance()->readAttribute(filehandle, attri, rid, attributeName, data);
	assert(rc == 0);

    return 0;
}

RC RelationManager::reorganizePage(const string &tableName, const unsigned pageNumber)
{
	if(!FileExists(tableName.c_str())){
		printf("%s doesn't exist!", tableName.c_str());
		return -1;
	}

	FileHandle filehandle;
	int rc;
	if (filehandles.find(tableName) != filehandles.end()) {
		filehandle = filehandles[tableName];
		filehandle.readPageInfo();
	} else {
		rc = RecordBasedFileManager::instance()->openFile(tableName, filehandle);
		assert(rc == 0);
		filehandles[tableName] = filehandle;
	}

	/*vector<Attribute> attri;
	int rc;
	if(attributes.find(tableName) != attributes.end()){
		attri = attributes[tableName];
	}else{
		rc = getAttributes(tableName, attri);
		assert(rc==0);
		attributes[tableName] = attri;
	}*/
	vector<Attribute> attrs;
	rc = RecordBasedFileManager::instance()->reorganizePage(filehandle, attrs, pageNumber);
	assert(rc == 0);
	rc = filehandle.savePageInfo();
	assert(rc == 0);
	printf("#### RE-ORGANIZE PAGE FINISHED! \n");
    return 0;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
	if(!FileExists(tableName.c_str())){
		printf("%s doesn't exist!", tableName.c_str());
		return -1;
	}

	FileHandle filehandle;
	int rc;
	if (filehandles.find(tableName) != filehandles.end()) {
		filehandle = filehandles[tableName];
		filehandle.readPageInfo();

	} else {
		rc = RecordBasedFileManager::instance()->openFile(tableName, filehandle);
		assert(rc == 0);
		filehandles[tableName] = filehandle;
	}

	vector<Attribute> attri;

	if(attributes.find(tableName) != attributes.end()){
		attri = attributes[tableName];
	}else{
		rc = getAttributes(tableName, attri);
		assert(rc==0);
		attributes[tableName] = attri;
	}

	rc = RecordBasedFileManager::instance()->scan(filehandle, attri, conditionAttribute,compOp,value,attributeNames,rm_ScanIterator);
    return rc;
}

// Extra credit
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}

// Extra credit
RC RelationManager::reorganizeTable(const string &tableName)
{
    return -1;
}
