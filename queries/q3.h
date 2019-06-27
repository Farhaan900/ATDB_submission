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
 * SELECT SUM(A),A
 * FROM Table1
 * GROUP BY A
 * ORDER BY A;
 */
namespace CoGaDB{

    int dummyPointer=100;
    int arr[100];
    int arradd[100];
    int arrcomp[101][2];
    int arrvolc[101][2];

    int arrcompfinal[101];
    int arrvolcfinal[101];

    //Compiled processing here
    int* compiledQ3(boost::shared_ptr<Column<int> > column_ptr){


    	for(int i=0;i<column_ptr->size();i++){
    	    		arr[i] = boost::any_cast<int>(column_ptr->get(i));
    	    	}


    	    	int n = sizeof(arr)/sizeof(arr[0]);

    	    	    std::sort(arr, arr+n);

    	    	    /**
    	    	    std::cout << "\nArray after sorting using "
    	    	         "default sort is : \n";
    	    	    for (int i = 0; i < 100; i++)
    	    	        std::cout << arr[i] << " ";

    	    	    std::cout<<"done printing"<<std::endl;
    	    	     */
    	    	    //using hash-map to store the sum of all similar values
    	    	    for (int i = 0; i < 100; i++)
    	    	    	arradd[arr[i]]+=arr[i];

    	    	    int counter = 1;

    	    	    /**
    	    	    std::cout<<"printing hashmap"<<std::endl;
    	    	    for (int i = 0; i < 100; i++)
    	    	        	    	        std::cout << i << " ";
    	    	    std::cout<<std::endl;

    	    	    */
    	    	    for (int i = 0; i < 100; i++){

    	    	        	    	        //std::cout << arradd[i] << " ";
    	    	        	    	        if(arradd[i] != 0){
    	    	        	    	            	    		arrcomp[0][0]++;
    	    	        	    	            	    		arrcomp[counter][0]=arradd[i];
    	    	        	    	            	    		arrcomp[counter][1]=i;
    	    	        	    	            	    		counter++;
    	    	        	    	            	    	}
    	    	    }

    	    	    //std::cout<<std::endl;

    	    	    /**
    	    	    for(int i=1; i<=arrcomp[0][0]; i++){
    	    	        	    	//std::cout<<arrcomp[i][0]<<"   "<<arrcomp[i][1]<<std::endl;
    	    	        	    }
					*/

    	    	    //generating array for comparison
    	    	    //first element tells the number of values
    	    	    arrcompfinal[0]=arrcomp[0][0];
    	    	    for(int i=1; i<=arrcomp[0][0]; i++){
    	    	    	arrcompfinal[i] = (arrcomp[i][0]*1000) + arrcomp[i][1];
    	    	    }

        return arrcompfinal;
    }

    //Volcano processing sequence here
    void executeQ3(){

    	 boost::shared_ptr<Column<int> > column_ptr (new Column<int>("int column",INT));//Input pointer
    	        std::vector<int> reference_data(100);
    	        fill_column<int>(column_ptr, reference_data);
    	                for(int i=0; i<100; i++){
    	                	std::cout<<" " <<reference_data[i];
    	                }

    	scan  s(column_ptr);
    	sorting sort(&s);
    	GroupedAggregation aggri(&sort);

    	aggri.open();

    	//std::cout<<"printing data << "<<std::endl;

    	/**
    	data = aggri.next();

    	std::cout<<"printing data <<  1 "<<std::endl;
    	while (data != EOL){
    		std::cout<<" " <<data;
    		data = aggri.next();
    	}
    	data = aggri.next();
    	std::cout<<"printing data << 2"<<std::endl;
    	while (data != EOL){
    	    		std::cout<<" " <<data;
    	    		data = aggri.next();
    	    	}
    	data = aggri.next();
    	std::cout<<"printing data << 3"<<std::endl;
    	while (data != EOL){
    	    		std::cout<<" " <<data;
    	    		data = aggri.next();
    	    	}
/***/
    	//keeps track of number of groups;
    	int counter = 1;

    	data = aggri.next();
    	while (data != EOL){


    		while (data != EOL){

    			//std::cout<<"  "<<data;
    			arrvolc[counter][1] = data;
    			data = aggri.next();
    		}
    		//std::cout<<" end part "<<std::endl;
    		counter ++;
    		data = aggri.next();

    	}


    	arrvolc[0][0] = counter-1;

    	aggri.close();

    	//std::cout<<"printing volcano values"<<std::endl;
    	for(int i=1; i<=arrvolc[0][0]; i++){
    	//    	    	        	    	std::cout<<arrcompvolc[i][0]<<"   "<<arrcompvolc[i][1]<<std::endl;
    	}

    	scan  s2(column_ptr);
    	sorting sort2(&s2);
    	GroupedAggregation aggri2(&sort2);
    	reduce r(&aggri2);

    	/**
    	r.open();
    	data = r.next();
    	std::cout<<" reduced value "<<data<<std::endl;
    	data = r.next();
    	std::cout<<" reduced value "<<data<<std::endl;
    	data = r.next();
    	std::cout<<" reduced value "<<data<<std::endl;
    	 */

    	r.open();
    	data = 0;
    	counter = 1;
    	while(data != EOL){
    		data = r.next();
    		arrvolc[counter][0] = data;
    		counter ++;
    		//std::cout<<" reduced value "<<data<<std::endl;

    	}

    	arrvolc[0][0] = counter -2;

    	/**
    	std::cout<<"printing volcano values"<<std::endl;
    	    	for(int i=1; i<=arrvolc[0][0]; i++){
    	    	    	    	        	    	std::cout<<arrvolc[i][0]<<"   "<<arrvolc[i][1]<<std::endl;
    	    	}
*/

    	//generating array for comparison
    	//first element tells the number of values
    	arrvolcfinal[0]=arrvolc[0][0];
    	for(int i=1; i<=arrvolc[0][0]; i++){
    	arrvolcfinal[i] = (arrvolc[i][0]*1000) + arrvolc[i][1];
    	}

        int* volcanoResult=arrvolcfinal;

        int* compiledResult=compiledQ3(column_ptr);

        std::cout<<"printing comparission values from compiled results"<<std::endl;
        for(int i = 1;i<compiledResult[0];i++){
        	std::cout<<"  "<<compiledResult[i]<<"  volcano  "<<volcanoResult[i]<<std::endl;
        }

        std::cout<<"  "<<compiledResult[0]<<"  volcano counter  "<<volcanoResult[0]<<std::endl;
        std::cout<<std::memcmp(volcanoResult, compiledResult,1)<<std::endl;//replace 1 with column size
    }
}
