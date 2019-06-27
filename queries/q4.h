//
// Created by gurumurt on 4/12/19.
//

#ifndef DB2_PROGRAMMING_PROJECT_Q1_H
#define DB2_PROGRAMMING_PROJECT_Q1_H

#endif //DB2_PROGRAMMING_PROJECT_Q1_H

#include <string>
#include <core/global_definitions.hpp>
#include <core/base_column.hpp>
#include <core/column_base_typed.hpp>
#include <core/column.hpp>
#include <core/compressed_column.hpp>
#include <processing/volcano.hpp>

/*
 * SELECT A
 * FROM Table1
 * JOIN Table2 ON A;
 */
namespace CoGaDB{

    int dummyPointer=100;
    std::vector<int> arr;
    std::vector<int> arr2;

    //Compiled processing here
    int* compiledQ4(boost::shared_ptr<Column<int> > column_ptr,boost::shared_ptr<Column<int> > column_ptr2){

    	int k=0;
    	for(int i=0;i<100;i++)
    	{
    		for(int j=0;j<100;j++ ){
    			if (boost::any_cast<int>(column_ptr->get(i)) == boost::any_cast<int>(column_ptr2->get(j))){
    				arr.push_back(boost::any_cast<int>(column_ptr->get(i)));
    				break;

    			}
    		}
    	}

    	std::cout<<std::endl<<"length of array"<<arr.size()<<std::endl;

    	//arr2.push_back(1);

    	//std::cout<<std::endl<<"length of array 2"<<arr2.size()<<std::endl;

    	for (std::vector<int>::const_iterator i = arr.begin(); i != arr.end(); ++i)
    	    std::cout << *i << ' ';

    	std::cout<<std::endl;

    	return arr.data();
    }

    //Volcano processing sequence here
    void executeQ4(){

    	 boost::shared_ptr<Column<int> > column_ptr (new Column<int>("int column",INT));//Input pointer
    	 std::vector<int> reference_data(100);
    	 fill_column<int>(column_ptr, reference_data);
    	 boost::shared_ptr<Column<int> > column_ptr2 (new Column<int>("int column",INT));//Input pointer
    	 //std::vector<int> reference_data(100);
    	 fill_column<int>(column_ptr2, reference_data);


    	 for(int i=0; i<100; i++){
    	      	std::cout<<" " <<boost::any_cast<int>(column_ptr->get(i));
    	 }
    	 std::cout<<std::endl;
    	 for(int i=0; i<100; i++){
    	     	std::cout<<" " <<boost::any_cast<int>(column_ptr2->get(i));
    	 }


    	 scan  s(column_ptr);
    	 scan  s2(column_ptr2);
    	 Join join(&s,&s2);

    	 join.open();

    	 int data = join.next();
    	 while (data != EOL){

    		 arr2.push_back(data);

    		 data = join.next();
    	 }

    	 join.close();

    	 for (std::vector<int>::const_iterator i = arr2.begin(); i != arr2.end(); ++i)
    	     	    std::cout << *i << ' ';

    	     	std::cout<<std::endl;

        int* volcanoResult=arr2.data();

        int* compiledResult=compiledQ4(column_ptr,column_ptr2);

        std::cout<<std::memcmp(volcanoResult, compiledResult,1)<<std::endl;//replace 1 with column size
    }

}
