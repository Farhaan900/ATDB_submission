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
 * ORDER BY A;
 */
namespace CoGaDB{

    int dummyPointer=100;
    int arr[100];
    std::vector<int> varr;


    //Compiled processing here
    int* compiledQ2(boost::shared_ptr<Column<int> > column_ptr){



    	for(int i=0;i<column_ptr->size();i++){
    		arr[i] = boost::any_cast<int>(column_ptr->get(i));
    	}


    	int n = sizeof(arr)/sizeof(arr[0]);

    	    std::sort(arr, arr+n);

    	    std::cout << "\nArray after sorting using "
    	         "default sort is : \n";
    	    for (int i = 0; i < 100; i++)
    	        std::cout << arr[i] << " ";

    	    std::cout<<"done printing"<<std::endl;



        return arr;
    }

    //Volcano processing sequence here
    void executeQ2(){

        boost::shared_ptr<Column<int> > column_ptr (new Column<int>("int column",INT));//Input pointer
        std::vector<int> reference_data(100);
        fill_column<int>(column_ptr, reference_data);
                for(int i=0; i<100; i++){
  //              	std::cout<<" " <<reference_data[i];
                }

        scan  s(column_ptr);
        sorting sort(&s);

        //s.open();
        sort.open();




        int tarray[100];
        int data;
        data = sort.next();
        int j=0;

        while (data!=EOL)
        {
        	std::cout<<" " <<data;
        	tarray[j]= data;
        	data = sort.next();
        	j++;
        }


        sort.close();

        std::cout<<std::endl<<"printing after filter"<<std::endl;

        int* volcanoResult=tarray;

        for(int i=0; i<100; i++){
                        	std::cout<<" " <<volcanoResult[i];
                        }
        int* compiledResult=compiledQ2(column_ptr);

        std::cout<<"printing result"<<std::endl;

        std::cout<<std::memcmp(compiledResult, volcanoResult,2)<<std::endl;//replace 1 with column size
    }

}
