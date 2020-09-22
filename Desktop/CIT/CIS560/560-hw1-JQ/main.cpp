#include <iostream>
#include "vec4.h"
#include "mat4.h"
#include <math.h>
using namespace std;


void testVec4Constructor1();     // test for all zeroes
void testVec4Constructor2(float x, float y, float z, float w);
void testVec4Constructor3(vec4 &v);
void testVec4Print(vec4 &v);
void testVec4Assign(vec4 &v1);
void testVec4CompareEqual(vec4 &v1, vec4 &v2, bool expected);
void testVec4CompareNotEqual(vec4 &v1, vec4 &v2, bool expected);
void testVec4DotProduct(vec4 &v1, vec4 &v2, float expected);
void testVec4CrossProduct(vec4 &v1, vec4 &v2, vec4 &expected);
void testVec4Normalize(vec4 &v1, vec4 &expected);


void testMat4Constructor1();      // test if it is identity
void testMat4Constructor2(float diag);
void testMat4Constructor3(vec4 &v1, vec4 &v2, vec4 &v3, vec4 &v4);
void testMat4Constructor4(mat4 &m);
void testMat4Print(mat4 &m);
void testMat4Assign(mat4 &m);
void testMat4CompareEqual(mat4 &m1, mat4 &m2, bool expected);
void testMat4CompareNotEqual(mat4 &m1, mat4 &m2, bool expected);
void testMat4Rotate(float angle, float x, float y, float z, mat4 &expected);
void testMat4MatVecMultiplication(mat4 &m, vec4 &v, vec4 &expected);
void testMat4MatMatMultiplication(mat4 &m1, mat4 &m2, mat4 &expected);
void testMat4VecMatMultiplication(vec4 &v, mat4 &m, vec4 &expected);

bool cmpf(float a, float b, float e = 0.00001f);

/** Main Function **/
int main()
{
    /******************
        vec4 TESTS
    ******************/
    vec4 i = vec4();
    vec4 v1 = vec4();
    v1[0] = 1;
    v1[1] = 2;
    v1[2] = 3;
    v1[3] = 4;
    vec4 v2 = vec4(2, 2, 2, 2);
    vec4 v3 = vec4(3,4,5,6);
    vec4 v4 = vec4(5,6,7,8);
    mat4 m1 = mat4();
    mat4 m2 = mat4(v1,v2,v3,v4);
    mat4 m3 = mat4(v3,v2,v2,v3);







    testVec4Constructor1();     // test for all zeroes
    testVec4Constructor2(1.f, 2.f, 3.f, 4.f);
    testVec4Constructor3(v1);

//    /* << */
    testVec4Print(v1);
//    /* = assign */
    testVec4Assign(v1);

//    /* == and != */
    testVec4CompareEqual(v1, v2, false);
    testVec4CompareEqual(i, i, true);
    testVec4CompareNotEqual(v1,v2, true);
    testVec4CompareNotEqual(i,i, false);

//    /* dot */
    testVec4DotProduct(v1, v2, 20);
    testVec4DotProduct(v1, i, 0);
//    /* cross */
    vec4 exp_cross = vec4(-2,4,-2,0);
    testVec4CrossProduct(v1, v2, exp_cross);
//    /* nromalize */
    vec4 norm_vec = vec4(2.f,2.f,2.f,0.f);
    vec4 exp_normalize = vec4(0.57735f,0.57735f,0.57735f,0.f);
    testVec4Normalize(norm_vec, exp_normalize);


//    /* *********
//     * mat4 TESTS
//     *
//     **********/

    testMat4Constructor1();      // test if it is identity
    testMat4Constructor2(2);
    testMat4Constructor3(v1, v2, v3,v4);
    testMat4Constructor4(m1);
    /* << */
    testMat4Print(m2);
    /* = */
    testMat4Assign(m2);
    /* == */
    testMat4CompareEqual(m1, m1, true);
    testMat4CompareEqual(m1, m2, false);
    /* != */
    testMat4CompareNotEqual(m1, m2, true);
    testMat4CompareNotEqual(m1, m1, false);

    /* rotate */
    /*
[  0.6835480, -0.3330434,  0.6494954;
   0.6494954,  0.6835480, -0.3330434;
  -0.3330434,  0.6494954,  0.6835480 ]

    */
    vec4 r1 = vec4(0.683548f, 0.649495f, -0.333043f,0.f);
    vec4 r2 = vec4(-0.333043f, 0.683548f,0.649495f, 0.f);
    vec4 r3 = vec4(0.649495f, -0.333043f,0.683548f, 0.f);
    vec4 r4 = vec4(0.f, 0.f,0.f, 1.f);
    mat4 exp_rotate_1 = mat4(r1,r2,r3,r4);
  //  cout <<"exp: \n" <<exp_rotate_1 ;
    /*
[  0.3064862,  0.9016235, -0.3051905;
  -0.4956642,  0.4248910,  0.7574857;
   0.8126397, -0.0808869,  0.5771257 ]

    */
    vec4 r5 = vec4(0.3064862f, -0.4956642f, 0.8126397f,0.f);
    vec4 r6 = vec4(0.9016235f, 0.4248910f,-0.0808869f, 0.f);
    vec4 r7 = vec4(-0.3051905f, 0.7574857f,0.5771257f, 0.f);
    vec4 r8 = vec4(0.f, 0.f,0.f, 1.f);

    mat4 exp_rotate_2 = mat4(r5,r6,r7,r8);
    testMat4Rotate(45.f, 2.f, 2.f, 2.f, exp_rotate_1);
    testMat4Rotate(30.f, 3.f, 4.f, 5.f, exp_rotate_2);


    /* m*v */
    vec4 exp_mul_mat_vec = vec4(2,2,2,2);
    testMat4MatVecMultiplication(m1, v2, exp_mul_mat_vec);
    /* m*m */
    mat4 exp_mul_mat_mat = mat4();
    exp_mul_mat_mat[0] = vec4(56,70,84,98);
    exp_mul_mat_mat[1] = vec4(22,28,34,40);
    exp_mul_mat_mat[2] = vec4(22,28,34,40);
    exp_mul_mat_mat[3] = vec4(56,70,84,98);
    testMat4MatMatMultiplication(m2, m3, exp_mul_mat_mat);
    /* v*m */
    vec4 exp_mul_vec_mat = vec4(50,36,86,122);
    testMat4VecMatMultiplication(v3, m2, exp_mul_vec_mat);
    return 0;
}










/****************************

Tests Implementation


*************************/
void testVec4Constructor1(){
    vec4 v = vec4();
    if( v[0]!=0 || v[1]!=0 || v[2]!=0 || v[3]!=0) {
        cout << "vec 4 constructor 1 failed !" << endl;
    }else{

        cout << "vec 4 constructor 1 passed! " << endl;
    }
}     // test for all zeroes



void testVec4Constructor2(float x, float y, float z, float w){
    vec4 v = vec4(x,y,z,w);
    if(v[0]!=x || v[1]!=y || v[2]!=z || v[3]!=w){
        cout << "vec 4 constructor 2 failed !" << endl;
    }else{
        cout << "vec 4 constructor 2 passed! " << endl;
    }
}
void testVec4Constructor3(vec4 &v2){
    vec4 v = vec4(v2);
    if(v!=v2) {
        cout << "vec 4 constructor 3 failed !" << endl;
    }else{
        cout << "vec 4 constructor 3 passed! " << endl;
    }
}
void testVec4Print(vec4 &v){
    cout << "testing print method for vec4" << endl;
    cout << v << endl;
}
void testVec4Assign(vec4 &v1){
    vec4 v = vec4();
    v = v1;
    if(v!=v1){
        cout << "vec 4 assign test failed !" << endl;
    }else{
        cout << "vec 4 assign test passed !"<< endl;
    }
}
void testVec4CompareEqual(vec4 &v1, vec4 &v2, bool expected){
    bool actual = v1 ==v2;
    if(expected != actual){
        cout << "vec 4 == test failed!" << endl;
    }else{
        cout << "vec 4 == test passed!" << endl;
    }
}
void testVec4CompareNotEqual(vec4 &v1, vec4 &v2, bool expected){
    bool actual = v1 !=v2;
    if(expected != actual){
        cout << "vec 4 == test failed!" << endl;
    }else{
        cout << "vec 4 == test passed!" << endl;
    }
}
void testVec4DotProduct(vec4 &v1, vec4 &v2, float expected){
    float actual = dot(v1,v2);
    if(expected != actual){
        cout << "vec 4 dot test failed!" << endl;
    }else{
        cout << "vec 4 dot test passed!" << endl;
    }
}
void testVec4CrossProduct(vec4 &v1, vec4 &v2, vec4 &expected){
    vec4 actual = cross(v1,v2);
    if(expected != actual){
        cout << "vec 4 crossProduct test failed!" << endl;
    }else{
        cout << "vec 4 crossProduct test passed!" << endl;
    }
}
void testVec4Normalize(vec4 &v1, vec4 &expected){
    vec4 actual = normalize(v1);

    if(expected != actual){
        cout << "vec 4 normalize test failed!" << endl;
    }else{
        cout << "vec 4 normalize test passed!" << endl;
    }
}


void testMat4Constructor1(){
    vec4 v1 = vec4(1,0,0,0);
    vec4 v2 = vec4(0,1,0,0);
    vec4 v3 = vec4(0,0,1,0);
    vec4 v4 = vec4(0,0,0,1);
    mat4 m = mat4();
    if(m[0]!=v1 || m[1]!=v2 || m[2]!=v3 || m[3]!=v4){
        cout << "mat4 constructor 1 test failed!" << endl;
    }else{
        cout << "mat4 constructor 1 test passed!" << endl;
    }
}      // test if it is identity
void testMat4Constructor2(float diag){
   mat4 m = mat4(diag);
   vec4 v1 = vec4(diag,0,0,0);
   vec4 v2 = vec4(0,diag,0,0);
   vec4 v3 = vec4(0,0,diag,0);
   vec4 v4 = vec4(0,0,0,diag);
   if(m[0]!=v1 || m[1]!=v2 || m[2]!=v3 || m[3]!=v4){
       cout << "mat4 constructor 2 test failed!" << endl;
   }else{
       cout << "mat4 constructor 2 test passed!" << endl;
   }
}
void testMat4Constructor3(vec4 &v1, vec4 &v2, vec4 &v3, vec4 &v4){
    mat4 m = mat4(v1,v2,v3,v4);
    if(m[0]!=v1 || m[1]!=v2 || m[2]!=v3 || m[3]!=v4){
        cout << "mat4 constructor 3 test failed!" << endl;
    }else{
        cout << "mat4 constructor 3 test passed!" << endl;
    }
}
void testMat4Constructor4(mat4 &m){
    mat4 m2 = mat4(m);
    if(m[0]!=m2[0] || m[1]!=m2[1] || m[2]!=m2[2] || m[3]!=m2[3]){
        cout << "mat4 constructor 4 test failed!" << endl;
    }else{
        cout << "mat4 constructor 4 test passed!" << endl;
    }
}
void testMat4Print(mat4 &m){
    cout << "testing print method for mat4" << endl;
    cout << m << endl;
}
void testMat4Assign(mat4 &m){
    mat4 m1 = m;
    if(m1!=m){
        cout << "mat 4 assign test failed !" << endl;
    }else{
        cout << "mat 4 assign test passed !"<< endl;
    }
}
void testMat4CompareEqual(mat4 &m1, mat4 &m2, bool expected){
    bool actual = m1 ==m2;
    if(expected != actual){
        cout << "mat 4 == test failed!" << endl;
    }else{
        cout << "mat 4 == test passed!" << endl;
    }
}
void testMat4CompareNotEqual(mat4 &m1, mat4 &m2, bool expected){
    bool actual = m1 !=m2;
    if(expected != actual){
        cout << "mat 4 != test failed!" << endl;
    }else{
        cout << "mat 4 != test passed!" << endl;
    }
}
void testMat4Rotate(float angle, float x, float y, float z, mat4 &expected){
    mat4 actual = mat4::rotate(angle, x, y, z);
         //  std::cout <<"rotation function : \n" << actual ;
    if(expected != actual){
        cout << "mat 4 rotate test failed!" << endl;
    }else{
        cout << "mat 4 rotate test passed!" << endl;
    }
}
void testMat4MatVecMultiplication(mat4 &m, vec4 &v, vec4 &expected){
        vec4 actual = m * v;
        if(expected != actual){
            cout << "mat 4 m*v test failed!" << endl;
        }else{
            cout << "mat 4 m*v test passed!" << endl;
        }
}
void testMat4MatMatMultiplication(mat4 &m1, mat4 &m2, mat4 &expected){
    mat4 actual = m1 * m2;
    if(expected != actual){
        cout << "mat 4 m1*m2 test failed!" << endl;
    }else{
        cout << "mat 4 m1*m2 test passed!" << endl;
    }
}
void testMat4VecMatMultiplication(vec4 &v, mat4 &m, vec4 &expected){
    vec4 actual = v*m;
    if(expected != actual){
        cout << "mat 4 v*m test failed!" << endl;
    }else{
        cout << "mat 4 v*m test passed!" << endl;
    }
}

bool cmpf(float A, float B, float epsilon)
{
    return (fabs(A - B) < epsilon);
}




