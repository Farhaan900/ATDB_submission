#pragma once

#include <string>
#include <core/global_definitions.hpp>
#include <core/base_column.hpp>
#include <core/column_base_typed.hpp>
#include <core/column.hpp>
#include <core/compressed_column.hpp>

#include "operator.hpp"

namespace CoGaDB{



    class volcanoOperator{

    public:
        volcanoOperator* child;
        volcanoOperator* child2;
        volcanoOperator(){
            this->child=NULL;
            this->child2=NULL;
        }
        virtual void open()=0;
        virtual int next()=0;
        virtual void close()=0;
        ~volcanoOperator(){};

    };

    class scan: public volcanoOperator{

        size_t i;
        boost::shared_ptr<Column<int> > column_ptr;

    public:

        scan(){}

        scan(boost::shared_ptr<Column<int> > ptr){

            this->column_ptr = ptr;
        }

        void open(){

            i=0;
            std::cout<<"open Column pointer"<<std::endl;
            std::cout<<"Column Size: "<<column_ptr->size()<<std::endl;
        };

        int next(){

            if(column_ptr->size() > i)

                return boost::any_cast<int>(column_ptr->get(i++));
            return EOL;
        };

        void close(){

            column_ptr.reset();
            i=0;
        }
    };

    class selection: public volcanoOperator{

        int operand;

    public:

        selection(){}
        selection(volcanoOperator* child,int oper = 10){

            this->child = child;
            operand = oper;
        }

        void open(){

            this->child->open();
        }

        int next(){
            int data = child->next();
            while(data !=EOL){
                if(data < operand)
                    return data;
                data = child->next();
            }
            return EOL;
        }
        void close(){}
    };

    class reduce: public volcanoOperator{

        int sum;
        bool flag;

    public:

        reduce(){}
        reduce(volcanoOperator* child){

            flag=false;
            this->child = child;
            sum = 0;
        }

        void open(){

            this->child->open();
/**
            int data = child->next();
            while(data !=EOL){
                sum+=data;
                data = child->next();
            }
   */
        }

        int next(){

        	int data = child->next();
        	if (data != EOL){
        		flag = false;
        	}
        	sum=0;
        	while(data !=EOL){
        		sum+=data;
        		data = child->next();
        	}

            if(!flag){
        	//if(data != EOL){
                flag=true;
                return sum;
            }

            return EOL;

        }
        void close(){
        }
    };

    class Join: public volcanoOperator{

    	int i=0;

    public:

        Join(){}
        Join(volcanoOperator* child,volcanoOperator* child2){

            this->child = child;
            this->child2 = child2;
        }

        void open(){

        	std::cout<<std::endl<<"in open block"<<std::endl;

        	        	this->child->open();



        }

        int next(){





        	data = child->next();
        	int data2;// = child2->next();

        	while(data != EOL)
        	{

        		this->child2->open();
        		data2 = child2->next();

        		while(data2 !=EOL){

        			if(data == data2){

        				//
        		        std::cout<<" "<<data;
        		        i++;

        		        // trying to close the pointer
        		        //this->child2->close();

        		        return data;
        		    }
        			data2 = child2->next();

        			i++;


        		}
        		data = child->next();
        		std::cout<<"data here "<<data<<std::endl;

        	}

        	return EOL;

        }

        void close(){
        }
    };

    class GroupedAggregation: public volcanoOperator{

    	int tarray[101][2]={0};
    	int i=0;
    	int data =0;
    	int pdata =0;
    	int k = 0;
    	int m = 0;
    	//to keep track of sets of numbers
    	int nextflag = 0;

    public:

        GroupedAggregation(){}
        GroupedAggregation(volcanoOperator* child){

            this->child = child;
        }

        void open(){

        	std::cout<<std::endl<<"in open block"<<std::endl;

        	        	this->child->open();
        	        	data = child->next();

        	        	tarray[0][0]=data;
        	        	tarray[0][1]=1;

        	        	while(data !=EOL){
        	        		data = child->next();
        	        		if( data == tarray[i][0]){

        	        			tarray[i][1] ++;
        	        			//i++;
        	        		}
        	        		else if (data != EOL) {
        	        			i++;
        	        			tarray[i][0]=data;
        	        			tarray[i][1]=1;
        	        		}
        	        		else{
        	        			i++;
        	        			tarray[i][0]=data;
        	        			tarray[i][1]=0;
        	        		}

        	        	}

        	        	//i=0;

        	        	//pdata = tarray[0];

        	        	/**
        	        	std::cout<<"printing data from open "<<std::endl;
        	        	for (int j = 0; j<=i; j++){
        	        		//std::cout <<tarray[j][0]<<"    "<<tarray[j][1]<<std::endl;
        	        	}
        	        	*/


        }

        int next(){

        	//data = tarray[i];

        	//i++;
/**
        	if (data == pdata)
        	{
        		//pdata = data;
        		return data;

        	}
        	else {
        		pdata = data;
        		return EOL;
        	}
*/

        	if (nextflag == 1){
        		//std::cout<<"next flag occurred << "<<std::endl;
        		nextflag = 0;
        		return EOL;
        	}
        	else if( tarray[k][1] != 0){
        		tarray[k][1] = tarray[k][1] -1 ;
        		m=k;
        		if(tarray[k][1] == 0){
        			nextflag = 1;
        			k++;
        			//std::cout<<"tarray [][] k count value  << "<<tarray[k][1]<<std::endl;
        		}
        		//std::cout<<"returning value  << "<<std::endl;
        		return tarray[m][0];
        	}
        	else {
        		//std::cout<<"final EOL -- value  << "<<std::endl;
        		return EOL;
        	}


        }

        void close(){

        	this->child->close();

        }
    };

    class sorting: public volcanoOperator{

    	int tarray[100]={0};
    	int i=0;
    	int data =0;


    public:

        sorting(){}
        sorting(volcanoOperator* child){


            this->child = child;

        }

        void open(){

        	//std::cout<<std::endl<<"in open block"<<std::endl;

        	this->child->open();
        	while(data !=EOL){
        		data = child->next();
        		if(data == EOL){
        			break;
        		}
        		tarray[i] = data;
        		i++;

        	}

        	int placeholder = i;
        	for(i;i<100;i++){
        		tarray[i] = 99999;
        	}



        	int n = sizeof(tarray)/sizeof(tarray[0]);

        	    	    std::sort(tarray, tarray+n);

        	i=0;

        	/*
        	std::cout<<"in open block printing array"<<std::endl;

        	for(int j=0; j<100; j++){
        	        	                        	std::cout<<" " <<tarray[j];
        	        	                        }
        	 */
        	if(placeholder < 100)
        	tarray[placeholder] = EOL;

        }

        int next(){

        	//std::cout<<"in next block"<<std::endl;

        	//int data = child->next();
        	        	//tarray[0] = data;


        	//int data = child->next();
        	//tarray[0] = data;
        	//std::cout<<"in next block  3 "<<data<<std::endl;
        	            while(i<100){
        	                //if(data < 200)
        	                  //  int x;
        	                	//return data;
        	                data = tarray[i];
        	            //    std::cout<<" "<<data<<std::endl;
        	                i++;
        	                return data;
        	            }
        	            return EOL;

        	//std::cout<<"in next block  2 "<<data<<std::endl;



        	//while(data !=EOL){

        	  //      		tarray[i] = data;
        	    //    	    data = child->next();
        	      //  	    i++;
        	        //	}

        	        	return 0;


        }

        void close(){
/**
        	std::cout<<"in close block"<<std::endl;
        	for(int j=0; j<100; j++){
        	                        	std::cout<<" " <<tarray[j];
        	                        }
        	*/

        }
    };




    class selection2: public volcanoOperator{

            int operandL;
            int operandR;

        public:

            selection2(){}
            selection2(volcanoOperator* child,int operL = 30,int operR = 80){

                this->child = child;
                operandL = operL;
                operandR = operR;
            }

            void open(){

                this->child->open();
            }

            int next(){
                int data = child->next();
                while(data !=EOL){
                    if(data < operandL || data > operandR)
                        return data;
                    data = child->next();
                }
                return EOL;
            }
            void close(){}
        };





}
