#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "ix.h"
#include "ixtest_util.h"

IndexManager *indexManager;

int testCase_4A(const string &indexFileName, const Attribute &attribute)
{
    // Functions tested
    // 1. Create Index File
    // 2. Open Index File
    // 3. Insert entry
    // 4. Scan entries NO_OP -- open**
    // 5. Scan close **
    // 6. Close Index File
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 4A****" << endl;

    RID rid;
    RC rc;
    IXFileHandle ixfileHandle;
    IX_ScanIterator ix_ScanIterator;
    unsigned key;
    unsigned numberOfPages = 4;
    int inRidPageNumSum = 0;
    int outRidPageNumSum = 0;
    unsigned numOfTuples = 1000;

    // create index file
    rc = indexManager->createFile(indexFileName, numberOfPages);
    if(rc == success)
    {
        cout << "Index File Created!" << endl;
    }
    else
    {
        cout << "Failed Creating Index File..." << endl;
        return fail;
    }

    // open index file
    rc = indexManager->openFile(indexFileName, ixfileHandle);
    if(rc == success)
    {
        cout << "Index File, " << indexFileName << " Opened!" << endl;
    }
    else
    {
        cout << "Failed Opening Index File..." << endl;
        return fail;
    }

    // insert entry
    for(unsigned i = 0; i <= numOfTuples; i++)
    {
        key = i+1;//just in case somebody starts pageNum and recordId from 1
        rid.pageNum = key;
        rid.slotNum = key+1;

        rc = indexManager->insertEntry(ixfileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Entry..." << endl;
            indexManager->closeFile(ixfileHandle);
            return fail;
        }
        inRidPageNumSum += rid.pageNum;
    }

    // Scan
    rc = indexManager->scan(ixfileHandle, attribute, NULL, NULL, true, true, ix_ScanIterator);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else
    {
        cout << "Failed Opening Scan!" << endl;
    	indexManager->closeFile(ixfileHandle);
    	return fail;
    }

    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {
    	if (rid.pageNum % 200 == 0) {
        	cout << rid.pageNum << " " << rid.slotNum << endl;
    	}
        outRidPageNumSum += rid.pageNum;
    }

    if (inRidPageNumSum != outRidPageNumSum)
    {
    	cout << "Wrong entries output...failure" << endl;
    	ix_ScanIterator.close();
    	return fail;
    }

    // Close Scan
    rc = ix_ScanIterator.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Scan..." << endl;
    	indexManager->closeFile(ixfileHandle);
    	return fail;
    }

    // Close Index
    rc = indexManager->closeFile(ixfileHandle);
    if(rc == success)
    {
        cout << "Index File Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Index File..." << endl;
    }

    return success;

}


int main()
{
    //Global Initializations
    indexManager = IndexManager::instance();

	const string indexFileName = "age_idx";
	Attribute attrAge;
	attrAge.length = 4;
	attrAge.name = "age";
	attrAge.type = TypeInt;

	RC result = testCase_4A(indexFileName, attrAge);
    if (result == success) {
    	cout << "IX_Test Case 4a passed" << endl;
    	return success;
    } else {
    	cout << "IX_Test Case 4a failed" << endl;
    	return fail;
    }

}

