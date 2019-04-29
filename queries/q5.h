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
 * SELECT sum(A),A
 * FROM Table1
 * WHERE A < xxx AND A > yyy
 * GROUP BY A
 * ORDER BY A;
 */
namespace CoGaDB{

    int dummyPointer=100;

    //Compiled processing here
    int* compiledQ5(boost::shared_ptr<Column<int> > column_ptr){
        return &dummyPointer;
    }

    //Volcano processing sequence here
    void executeQ5(){

        boost::shared_ptr<Column<int> > column_ptr;//Input pointer
        int* volcanoResult=&dummyPointer;

        int* compiledResult=compiledQ2(column_ptr);

        std::cout<<std::memcmp(volcanoResult, compiledResult,1)<<std::endl;//replace 1 with column size
    }
}
