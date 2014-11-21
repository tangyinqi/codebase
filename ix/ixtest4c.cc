#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "ix.h"
#include "ixtest_util.h"

IndexManager *indexManager;

int testCase_4C(const string &indexFileName, const Attribute &attribute)
{
    // Functions tested
    // 1. Create Index File
    // 2. Open Index File
    // 3. Insert entry
    // 4. Scan entries EXACT MATCH **
    // 5. Scan close **
    // 6. Close Index File
    // 7. Destroy Index File
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 4C****" << endl;

    RID rid;
    RC rc;
    IXFileHandle ixfileHandle;
    IX_ScanIterator ix_ScanIterator;
    unsigned key = 200;
    unsigned numberOfPages = 4;
    int inRidPageNumSum = 0;
    int outRidPageNumSum = 0;
    unsigned numOfTuples = 200;

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
        rid.pageNum = i+1;
        rid.slotNum = i+2;

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
    rc = indexManager->scan(ixfileHandle, attribute, &key, &key, true, true, ix_ScanIterator);
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
    	if (rid.pageNum % 20 == 0) {
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
    	indexManager->destroyFile(indexFileName);
    	return fail;
    }

    // destroy index file
    rc = indexManager->destroyFile(indexFileName);
    if(rc != success)
    {
        cout << "Failed Destroying Index File..." << endl;
        return fail;
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

	RC result = testCase_4C(indexFileName, attrAge);
    if (result == success) {
    	cout << "IX_Test Case 4C passed" << endl;
    	return success;
    } else {
    	cout << "IX_Test Case 4C failed" << endl;
    	return fail;
    }

}

