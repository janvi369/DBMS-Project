#include "Algebra.h"
#include <cstring>
#include<cstdlib>
#include<cstdio>

bool isNumber(char *str);
/* used to select all the records that satisfy a condition.
the arguments of the function are
- srcRel - the source relation we want to select from
- targetRel - the relation we want to select into. (ignore for now)
- attr - the attribute that the condition is checking
- op - the operator of the condition
- strVal - the value that we want to compare against (represented as a string)
*/

int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE])
{	
	int srcRelId = OpenRelTable::getRelId(srcRel);      // we'll implement this later
	if (srcRelId == E_RELNOTOPEN)
	{
		return E_RELNOTOPEN;
	}

	AttrCatEntry  attrCatEntry;
	// get the attribute catalog entry for attr, using AttrCacheTable::getAttrcatEntry()
	//    return E_ATTRNOTEXIST if it returns the error
	if ( AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry)==E_ATTRNOTEXIST )
		return E_ATTRNOTEXIST;
	
	/*** Convert strVal (string) to an attribute of data type NUMBER or STRING ***/
	int type = attrCatEntry.attrType;
	Attribute attrVal;
	if (type == NUMBER)
	{
		if (isNumber(strVal))
		{       // the isNumber() function is implemented below
			attrVal.nVal = atof(strVal);
		}
		else
		{
			return E_ATTRTYPEMISMATCH;
		}
	}
	else if (type == STRING)
	{
		strcpy(attrVal.sVal, strVal);
	}

	/*** Selecting records from the source relation ***/

	// Before calling the search function, reset the search to start from the first hit
	// using RelCacheTable::resetSearchIndex()
	//RelCacheTable::resetSearchIndex(crcRelId);
	
	RelCatEntry relCatEntry;
	// get relCatEntry using RelCacheTable::getRelCatEntry()
	RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);
	int src_nAttrs = relCatEntry.numAttrs ;
	
	/* let attr_names[src_nAttrs][ATTR_SIZE] be a 2D array of type char
        (will store the attribute names of rel). */
        char attr_names[src_nAttrs][ATTR_SIZE];
    	// let attr_types[src_nAttrs] be an array of type int
    	int attr_types[src_nAttrs];
    	
	/*iterate through 0 to src_nAttrs-1 :
	get the i'th attribute's AttrCatEntry using AttrCacheTable::getAttrCatEntry()
	fill the attr_names, attr_types arrays that we declared with the entries
	of corresponding attributes
	*/
	for (int i = 0; i < src_nAttrs; ++i)
	{
		AttrCatEntry attrCatEntry;
		// get attrCatEntry at offset i using AttrCacheTable::getAttrCatEntry()
		AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);

		strcpy(attr_names[i],attrCatEntry.attrName);
		attr_types[i]=attrCatEntry.attrType;
	}
	
	/* Create the relation for target relation by calling Schema::createRel()
	by providing appropriate arguments */
	// if the createRel returns an error code, then return that value.
	int ret=Schema::createRel(targetRel, src_nAttrs, attr_names,attr_types);
	if(ret!=SUCCESS)
		return ret;
		
	/* Open the newly created target relation by calling OpenRelTable::openRel()
	method and store the target relid */
	int targetRelId=OpenRelTable::openRel(targetRel);
	/* If opening fails, delete the target relation by calling Schema::deleteRel()
	and return the error value returned from openRel() */
	if(targetRelId<0 || targetRelId>=MAX_OPEN)
	{
		Schema::deleteRel(targetRel);
		return targetRelId;
	}
	/*** Selecting and inserting records into the target relation ***/
	/* Before calling the search function, reset the search to start from the
	first using RelCacheTable::resetSearchIndex() */
	
	Attribute record[src_nAttrs];
	
	/*
        The BlockAccess::search() function can either do a linearSearch or
        a B+ tree search. Hence, reset the search index of the relation in the
        relation cache using RelCacheTable::resetSearchIndex().
        Also, reset the search index in the attribute cache for the select
        condition attribute with name given by the argument `attr`. Use
        AttrCacheTable::resetSearchIndex().
        Both these calls are necessary to ensure that search begins from the
        first record.
	*/
	RelCacheTable::resetSearchIndex(srcRelId);
	AttrCacheTable::resetSearchIndex(srcRelId,attr);

	// read every record that satisfies the condition by repeatedly calling
	// BlockAccess::search() until there are no more records to be read
	
	while (BlockAccess::search(srcRelId, record, attr,attrVal, op)==SUCCESS)
	{

		ret = BlockAccess::insert(targetRelId, record);

		if (ret!=SUCCESS)
		{
			Schema::closeRel(targetRel);
			Schema::deleteRel(targetRel);
			return ret;
		}
	}

	// Close the targetRel by calling closeRel() method of schema layer
	Schema::closeRel(targetRel);

	return SUCCESS;
}

// will return if a string can be parsed as a floating point number
bool isNumber(char *str)
{
	int len;
	float ignore;
	/*
	sscanf returns the number of elements read, so if there is no float matching
	the first %f, ret will be 0, else it'll be 1

	%n gets the number of characters read. this scanf sequence will read the
	first float ignoring all the whitespace before and after. and the number of
	characters read that far will be stored in len. if len == strlen(str), then
	the string only contains a float with/without whitespace. else, there's other
	characters.
	*/
	int ret = sscanf(str, "%f %n", &ignore, &len);
	return ret == 1 && len == strlen(str);
}

int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE])
{
	if(strcmp(relName,RELCAT_RELNAME)==0 ||strcmp(relName,ATTRCAT_RELNAME)==0)
		return E_NOTPERMITTED;
		
	int relId = OpenRelTable::getRelId(relName);

	if (relId==E_RELNOTOPEN)
		return E_RELNOTOPEN;
	// get the relation catalog entry from relation cache
	// (use RelCacheTable::getRelCatEntry() of Cache Layer)
	RelCatEntry relcatentry;
	RelCacheTable::getRelCatEntry(relId,&relcatentry);

	/* if relCatEntry.numAttrs != numberOfAttributes in relation,
	return E_NATTRMISMATCH */
	if(relcatentry.numAttrs!=nAttrs)
		return E_NATTRMISMATCH;

	// let recordValues[numberOfAttributes] be an array of type union Attribute
	union Attribute recordValues[nAttrs];
	/*
	Converting 2D char array of record values to Attribute array recordValues
	*/
	for(int i=0;i<nAttrs;++i)
	{
		// get the attr-cat entry for the i'th attribute from the attr-cache
		// (use AttrCacheTable::getAttrCatEntry())
		AttrCatEntry attrcatentry;
		AttrCacheTable::getAttrCatEntry(relId, i, &attrcatentry);
		// let type = attrCatEntry.attrType;
		int type=attrcatentry.attrType;
		if (type==NUMBER)
		{
			// if the char array record[i] can be converted to a number
			// (check this using isNumber() function)
			if(isNumber(record[i]))
			{
				/* convert the char array to numeral and store it
				at recordValues[i].nVal using atof() */
				recordValues[i].nVal=atof(record[i]);
			}
			else
			{
				return E_ATTRTYPEMISMATCH;
			}
		}
		else if (type == STRING)
		{
			// copy record[i] to recordValues[i].sVal
			strcpy(recordValues[i].sVal,record[i]);
		}
	}
	// insert the record by calling BlockAccess::insert() function
	// let retVal denote the return value of insert call
	int ret=BlockAccess::insert(relId, recordValues);
	return ret;

}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE])
{

	int srcRelId = OpenRelTable::getRelId(srcRel);

	// if srcRel is not open in open relation table, return E_RELNOTOPEN
	if (srcRelId < 0 || srcRelId >= MAX_OPEN)
		return E_RELNOTOPEN;
		
	RelCatEntry RelCatEntrySrcRel;
	// get RelCatEntry of srcRel using RelCacheTable::getRelCatEntry()
	RelCacheTable::getRelCatEntry(srcRelId,&RelCatEntrySrcRel);
	// get the no. of attributes present in relation from the fetched RelCatEntry.
	int numAttrs=RelCatEntrySrcRel.numAttrs;

	// attrNames and attrTypes will be used to store the attribute names
	// and types of the source relation respectively
	char attrNames[numAttrs][ATTR_SIZE];
	int attrTypes[numAttrs];

	/*iterate through every attribute of the source relation :
	- get the AttributeCat entry of the attribute with offset.
	(using AttrCacheTable::getAttrCatEntry())
	- fill the arrays `attrNames` and `attrTypes` that we declared earlier
	with the data about each attribute
	*/
	
	for(int i=0;i<numAttrs;++i)
	{
		AttrCatEntry AttrCatEntrySrcRel;
		AttrCacheTable::getAttrCatEntry(srcRelId, i, &AttrCatEntrySrcRel);
		strcpy(attrNames[i],AttrCatEntrySrcRel.attrName);
		attrTypes[i]=AttrCatEntrySrcRel.attrType;
	}
		
	/*** Creating and opening the target relation ***/

	// Create a relation for target relation by calling Schema::createRel()
	int ret=Schema::createRel(targetRel, numAttrs, attrNames, attrTypes);
	if(ret!=SUCCESS)
		return ret;
	// Open the newly created target relation by calling OpenRelTable::openRel()
	// and get the target relid
	int targetrelId=OpenRelTable::openRel(targetRel);

	// If opening fails, delete the target relation by calling Schema::deleteRel() of
	// return the error value returned from openRel().
	if(targetrelId<0 || targetrelId>=MAX_OPEN)
	{
		Schema::deleteRel(targetRel);
		return targetrelId;
	}

	/*** Inserting projected records into the target relation ***/

	// Take care to reset the searchIndex before calling the project function
	// using RelCacheTable::resetSearchIndex()
	RelCacheTable::resetSearchIndex(srcRelId);
	Attribute record[numAttrs];


	while ( BlockAccess::project(srcRelId, record)==SUCCESS)
	{
		// record will contain the next record

		ret = BlockAccess::insert(targetrelId, record);

		if (ret!=SUCCESS)
		{
			// close the targetrel by calling Schema::closeRel()
			Schema::closeRel(targetRel);
			// delete targetrel by calling Schema::deleteRel()
			Schema::deleteRel(targetRel);
			return ret;
		}
	}

	// Close the targetRel by calling Schema::closeRel()
	Schema::closeRel(targetRel);

	return SUCCESS;
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], int tar_nAttrs, char tar_Attrs[][ATTR_SIZE])
{

	int srcRelId = OpenRelTable::getRelId(srcRel);

	// if srcRel is not open in open relation table, return E_RELNOTOPEN
	if (srcRelId < 0 || srcRelId >= MAX_OPEN)
		return E_RELNOTOPEN;

	// get RelCatEntry of srcRel using RelCacheTable::getRelCatEntry()
	RelCatEntry RelCatEntrySrcRel;
	RelCacheTable::getRelCatEntry(srcRelId,&RelCatEntrySrcRel);

	// get the no. of attributes present in relation from the fetched RelCatEntry.
	int src_nAttrs= RelCatEntrySrcRel.numAttrs;

	// declare attr_offset[tar_nAttrs] an array of type int.
	// where i-th entry will store the offset in a record of srcRel for the
	// i-th attribute in the target relation.
	int attr_offset[tar_nAttrs];

	// let attr_types[tar_nAttrs] be an array of type int.
	// where i-th entry will store the type of the i-th attribute in the
	// target relation.
	int attr_types[tar_nAttrs];

	/*** Checking if attributes of target are present in the source relation
	and storing its offsets and types ***/

	/*iterate through 0 to tar_nAttrs-1 :
	- get the attribute catalog entry of the attribute with name tar_attrs[i].
	- if the attribute is not found return E_ATTRNOTEXIST
	- fill the attr_offset, attr_types arrays of target relation from the
	corresponding attribute catalog entries of source relation
	*/
	for(int i=0; i<tar_nAttrs; ++i)
	{
		AttrCatEntry AttrCatEntrySrcRel;
		int ret=AttrCacheTable::getAttrCatEntry(srcRelId, tar_Attrs[i], &AttrCatEntrySrcRel);
		if(ret== E_ATTRNOTEXIST)
			return ret;
		attr_offset[i]=AttrCatEntrySrcRel.offset;
		attr_types[i]=AttrCatEntrySrcRel.attrType;
	}
	
	/*** Creating and opening the target relation ***/

	// Create a relation for target relation by calling Schema::createRel()
	int ret=Schema::createRel(targetRel, tar_nAttrs, tar_Attrs, attr_types);
	if(ret!=SUCCESS)
		return ret;
	// Open the newly created target relation by calling OpenRelTable::openRel()
	// and get the target relid
	int targetRelId=OpenRelTable::openRel(targetRel);
	if(targetRelId<0 || targetRelId>=MAX_OPEN)
	{
		Schema::deleteRel(targetRel);
		return targetRelId;
	}

	/*** Inserting projected records into the target relation ***/

	// Take care to reset the searchIndex before calling the project function
	// using RelCacheTable::resetSearchIndex()
	RelCacheTable::resetSearchIndex(srcRelId);

	Attribute record[src_nAttrs];

	while (BlockAccess::project(srcRelId, record)==SUCCESS)
	{
		// the variable `record` will contain the next record

		Attribute proj_record[tar_nAttrs];

		//iterate through 0 to tar_attrs-1:
		//    proj_record[attr_iter] = record[attr_offset[attr_iter]]
		for(int i=0; i<tar_nAttrs; ++i)
		{
			 proj_record[i] = record[attr_offset[i]];
		}
		

		ret = BlockAccess::insert(targetRelId, proj_record);

		if (ret!=SUCCESS)
		{
			// close the targetrel by calling Schema::closeRel()
			Schema::closeRel(targetRel);
			// delete targetrel by calling Schema::deleteRel()
			Schema::deleteRel(targetRel);
			return ret;
		}
	}
	// Close the targetRel by calling Schema::closeRel()
	Schema::closeRel(targetRel);

	return SUCCESS;
}
int Algebra::join(char srcRelation1[ATTR_SIZE], char srcRelation2[ATTR_SIZE], char targetRelation[ATTR_SIZE], char attribute1[ATTR_SIZE], char attribute2[ATTR_SIZE]){

    // get relation1's and relation2's relId
    int relId1 = OpenRelTable::getRelId(srcRelation1);
    int relId2 = OpenRelTable::getRelId(srcRelation2);

    if(relId1 < 0 || relId1 >= MAX_OPEN || relId2 < 0 || relId2 >= MAX_OPEN){
        return E_RELNOTOPEN;
    }

    // get attribute catalog entries for the source relations corresponding to their give attributes.
    AttrCatEntry attrCatEntry1, attrCatEntry2;
    if(AttrCacheTable::getAttrCatEntry(relId1, attribute1, &attrCatEntry1) == E_ATTRNOTEXIST){
        return E_ATTRNOTEXIST;
    }
    if(AttrCacheTable::getAttrCatEntry(relId2, attribute2, &attrCatEntry2) == E_ATTRNOTEXIST){
        return E_ATTRNOTEXIST;
    }
    
    // if attribute1 and attribute2 are of different types, return error
    if(attrCatEntry1.attrType != attrCatEntry2.attrType){
        return E_ATTRTYPEMISMATCH;
    }

    /*
        iterate through all the attributes in both the source relation and check if there are
        any other pair of attributes other than join attributes

        if yes, then return error
    */
    RelCatEntry relCatEntry1, relCatEntry2;
    RelCacheTable::getRelCatEntry(relId1, &relCatEntry1);
    RelCacheTable::getRelCatEntry(relId2, &relCatEntry2);

    AttrCatEntry temp1, temp2;
    for(int j = 0; j < relCatEntry2.numAttrs; j++){
        if(j == attrCatEntry2.offset){
            continue;
        }

        AttrCacheTable::getAttrCatEntry(relId2, j, &temp2);
        
        for(int i = 0; i < relCatEntry1.numAttrs; i++){
            AttrCacheTable::getAttrCatEntry(relId1, i, &temp1);

            if(strcmp(temp2.attrName, temp1.attrName) == 0){

                return E_DUPLICATEATTR;
            }
        }
    }

    int numOfAttributes1 = relCatEntry1.numAttrs;
    int numOfAttributes2 = relCatEntry2.numAttrs;

    /*
        If srcRelation2 doesn't have an index on attribute2, create index

        This will reduce time complexity from O(m.n) -> O(mlogn + n)
        where m = no. of records in relation_1, n = no. of records in relation_2
    */
    if(attrCatEntry2.rootBlock == -1){
        int ret = BPlusTree::bPlusCreate(relId2, attribute2);
        if(ret != SUCCESS){
            return E_DISKFULL;
        }
    }

    int numOfAttributesInTarget = numOfAttributes1 + numOfAttributes2 - 1;
    
    // declaring the following arrays to store the details of the target relation
    char targetRelAttrNames[numOfAttributesInTarget][ATTR_SIZE];
    int targetRelAttrTypes[numOfAttributesInTarget];

    /*
        Iterate through all the attributes in both the source relations and update
        targetRelAttrNames[], targetRelAttrTypes[] arrays excluding attribute2
    */
    for(int i = 0; i < numOfAttributes1; i++){
        AttrCacheTable::getAttrCatEntry(relId1, i, &temp1);
        strcpy(targetRelAttrNames[i], temp1.attrName);
        targetRelAttrTypes[i] = temp1.attrType;
    }
    for(int j = 0; j < attrCatEntry2.offset; j++){
        AttrCacheTable::getAttrCatEntry(relId2, j, &temp2);
        strcpy(targetRelAttrNames[numOfAttributes1 + j], temp2.attrName);
        targetRelAttrTypes[numOfAttributes1 + j] = temp2.attrType;
    }
    for(int j = attrCatEntry2.offset + 1; j < numOfAttributes2; j++){
        AttrCacheTable::getAttrCatEntry(relId2, j, &temp2);
        strcpy(targetRelAttrNames[numOfAttributes1 + j - 1], temp2.attrName);
        targetRelAttrTypes[numOfAttributes1 + j - 1] = temp2.attrType;
    }

    // create the target relation
    int ret = Schema::createRel(targetRelation, numOfAttributesInTarget, targetRelAttrNames, targetRelAttrTypes);
    if(ret != SUCCESS){
        return ret;
    }

    // open and get targetRelation id
    int targetRelId = OpenRelTable::openRel(targetRelation);
    if(targetRelId < 0 || targetRelId >= MAX_OPEN){
        // delete the relation
        Schema::deleteRel(targetRelation);
        return targetRelId;
    }

    Attribute record1[numOfAttributes1];
    Attribute record2[numOfAttributes2];
    Attribute targetRecord[numOfAttributesInTarget];

    RelCacheTable::resetSearchIndex(relId1);

    // outer while loop to get all the records from srcRelation1 one by one
    while(BlockAccess::project(relId1, record1) == SUCCESS){

        // reset search index for srcRelation2, cause for every record in srcRelation1, we start from beginning
        RelCacheTable::resetSearchIndex(relId2);

        // reset search index for attribute2, cause we want record with whose attribute2 matches with attribute1
        AttrCacheTable::resetSearchIndex(relId2, attribute2);

        /*
            inner loop will get every record of srcRelation2 which satisfy the following condition:
            record1.attribute1 = record2.attribute2
        */
        while(BlockAccess::search(relId2, record2, attribute2, record1[attrCatEntry1.offset], EQ) == SUCCESS){
            /*
                copy srcRelation1's anf srcRelation2's attribute values (except for attribute2 in rel2) from record1
                and record2 to targetRecord
            */
            for(int i = 0; i < numOfAttributes1; i++){
                targetRecord[i] = record1[i];
            }
            for(int i = 0; i < attrCatEntry2.offset; i++){
                targetRecord[numOfAttributes1 + i] = record2[i];
            }
            for(int i = attrCatEntry2.offset + 1; i < numOfAttributes2; i++){
                targetRecord[numOfAttributes1 + i - 1] = record2[i];
            }

            // insert the currnt record into the target relation
            ret = BlockAccess::insert(targetRelId, targetRecord);
            if(ret != SUCCESS){
                OpenRelTable::closeRel(targetRelId);
                Schema::deleteRel(targetRelation);
                return E_DISKFULL;
            }
        }
    }

    OpenRelTable::closeRel(targetRelId);
    return SUCCESS;
}

