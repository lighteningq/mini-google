#include "vec4.h"
#include <math.h>

///----------------------------------------------------------------------
/// Constructors
///---------------------------------------------------------------------
vec4::vec4()
    :data{0,0,0,0}
{}

    vec4::vec4(float x, float y, float z, float w)
    :data{0,0,0,0}{
        this->data[0]=x;
        this->data[1]=y;
        this->data[2]=z;
        this->data[3]=w;
    }

    vec4::vec4(const vec4 &v2){
        for(unsigned int i = 0; i<4; i++){
            this->data[i] = v2.data[i];
        }
    }



    ///----------------------------------------------------------------------
    /// Getters/Setters
    ///----------------------------------------------------------------------
    /// Returns the value at index
    float vec4::operator[](unsigned int index) const{
        if(index >= 4) throw std::out_of_range("index out of range in getting value");
        return this->data[index];
    }

    /// Returns a reference to the value at index
    float& vec4::operator[](unsigned int index){
        if(index >= 4) throw std::out_of_range("index out of range in getting value");
        float& res = this->data[index];
        return res;
    }


    ///----------------------------------------------------------------------
    /// Operator Functions
    ///----------------------------------------------------------------------

    /// Assign v2 to this and return it
    vec4& vec4::operator=(const vec4 &v2){
        (*this)[0]=v2[0];
        (*this)[1]=v2[1];
        (*this)[2]=v2[2];
        (*this)[3]=v2[3];
        return *this;

    }
    bool cmpf_helper(float A, float B, float epsilon=0.001f)
    {
        return (fabs(A - B) < epsilon);
    }

    /// Test for equality
    ///  //Component-wise comparison
    bool vec4::operator==(const vec4 &v2) const{
        for(unsigned int i = 0; i<4; i++){
            if(!cmpf_helper(this->data[i],v2.data[i])) return false;
        }
        return true;
    }


    /// Test for inequality
    /// //Component-wise comparison
    bool vec4::operator!=(const vec4 &v2) const{
        for(unsigned int i = 0; i<4; i++){
            if(!cmpf_helper(this->data[i],v2.data[i])) return true;
        }
        return false;
    }



    /// Arithmetic:
    /// e.g. += adds v2 to this and return this (like regular +=)
    ///      +  returns a new vector that is sum of this and v2
    vec4& vec4::operator+=(const vec4 &v2){

        for(unsigned int i = 0; i<4; i++){
            this->data[i] += v2.data[i];
        }
        vec4 &res = *this;
        return res;
    }
    vec4& vec4::operator-=(const vec4 &v2){
        for(unsigned int i = 0; i<4; i++){
            this->data[i] -= v2.data[i];
        }
        vec4 &res = *this;
        return res;

    }
    // multiplication by a scalar
    vec4& vec4::operator*=(float c){
        for(unsigned int i = 0; i<4; i++){
            this->data[i] *= c;
        }
        vec4 &res = *this;
        return res;

    }

     // division by a scalar
    vec4& vec4::operator/=(float c){
        for(unsigned int i = 0; i<4;i++){
            this->data[i] /=c;
        }
        vec4 &res = *this;
        return res;
    }

    vec4  vec4::operator+(const vec4 &v2) const{
        vec4 res = vec4(v2);
        for(unsigned int i = 0; i<4;i++){
            res[i] += this->data[i];
        }
        return res;

    }
    vec4  vec4::operator-(const vec4 &v2) const{
        vec4 res = vec4(*this);
        for(unsigned int i = 0; i<4;i++){
            res[i] -= v2[i];

        }
        return res;
    }

     // multiplication by a scalar
    vec4  vec4::operator*(float c) const{
        vec4 res = vec4(*this);
        for(unsigned int i = 0; i<4;i++){
            res[i]*=c;

        }
        return res;
    }
    // division by a scalar
    vec4  vec4::operator/(float c) const{
         vec4 res = vec4(*this);
       for(unsigned int i = 0; i<4;i++){
           res[i]*=c;

       }
       return res;
    }

/// Dot Product
    float dot(const vec4 &v1, const vec4 &v2){
        float res = 0.f;
        for(unsigned int i = 0; i<4;i++){
            res+=v1[i]*v2[i];
        }
        return res;
    }

/// Cross Product
///
 //Compute the result of v1 x v2 using only their X, Y, and Z elements.
    //In other words, treat v1 and v2 as 3D vectors, not 4D vectors.
    //The fourth element of the resultant vector should be 0.
vec4 cross(const vec4 &v1, const vec4 &v2){
    vec4 res = vec4();
    res[0] = v1[1]*v2[2]-v1[2]*v2[1];
    res[1] = v1[2]*v2[0]-v1[0]*v2[2];
    res[2] = v1[0]*v2[1]-v1[1]*v2[0];

    return res;

}

/// Returns the geometric length of the input vector
float length(const vec4 &v){
    return (float) sqrt(v[0]*v[0] + v[1]*v[1] + v[2]*v[2] + v[3] * v[3]);
}

/// Scalar Multiplication (c * v)
vec4 operator*(float c, const vec4 &v){
    vec4 res = vec4(v);
    for(unsigned int i = 0; i<4;i++){
       res[i]*=c;
    }
    return res;
}

vec4 normalize(const vec4& v){
    float len = length(v);
    if(len== 0.f) throw std::out_of_range("input vector is a zero vector, invalid");
    vec4 res = vec4(v);
    for(unsigned int i = 0; i<4;i++){
        res[i]/=len;
    }
    return res;
}



/// Prints the vector to a stream in a nice format
std::ostream &operator<<(std::ostream &o, const vec4 &v){
    for(unsigned int i = 0; i< 4; i++){
        o << v[i] << "\r\n";
    }
    return o;
}
