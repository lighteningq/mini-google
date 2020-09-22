#include "mat4.h"
#include "vec4.h"
#include <math.h>
   ///----------------------------------------------------------------------
   /// Constructors
   ///----------------------------------------------------------------------
   /// Default Constructor.  Initialize to identity matrix.
mat4::mat4() :data{vec4(1,0,0,0),vec4(0,1,0,0),vec4(0,0,1,0),vec4(0,0,0,1)}{}

   /// Initializes the diagonal values of the matrix to diag. All other values are 0.
   mat4::mat4(float diag){
       for(unsigned int i = 0; i<4; i++){
           this->data[i] = vec4();
           this->data[i][i]=diag;
       }
   }

   /// Initializes matrix with each vector representing a column in the matrix
   mat4::mat4(const vec4 &col0, const vec4 &col1, const vec4 &col2, const vec4& col3){
         this->data[0]=col0;
       this->data[1]=col1;
       this->data[2]=col2;
       this->data[3]=col3;
   }
   mat4::mat4(const mat4 &m2){
       // copy constructor
     this->data[0]=m2[0];
     this->data[1]=m2[1];
     this->data[2]=m2[2];
     this->data[3]=m2[3];
    }
   ///----------------------------------------------------------------------
   /// Getters
   ///----------------------------------------------------------------------
   /// Returns the values of the column at the index
   vec4  mat4::operator[](unsigned int index) const{
       if(index>3 || index < 0) throw std::out_of_range("mat4 getting index out of range");
        return this->data[index];
   }

   /// Returns a reference to the column at the index
   vec4& mat4::operator[](unsigned int index){
       if(index>3 || index < 0) throw std::out_of_range("mat4 getting index out of range");
       vec4& v = this->data[index];
       return v;
   }

   ///----------------------------------------------------------------------
   /// Static Initializers
   ///----------------------------------------------------------------------
   /// Creates a 3-D rotation matrix.
   /// Takes an angle in degrees and an axis represented by its xyz components, and outputs a 4x4 rotation matrix
   /// Use Rodrigues' formula to implement this method
    mat4 mat4::rotate(float angle, float x, float y, float z){
       vec4 v_temp = vec4(x,y,z,0);
       vec4 v = normalize(v_temp);

        mat4 res = mat4();
        res[0][0] =  cos(angle) + (v[0]*v[0])*(1-cos(angle));

        res[0][1] = v[2] * sin(angle) + v[0] * v[1] * (1-cos(angle));
        res[0][2] = -v[1] * sin(angle) + v[0]*v[2] * (1-cos(angle));

        res[0][3] = 0.f;

        res[1][0] = -v[2] * sin(angle) + v[0] * v[1] *(1-cos(angle));
        res[1][1] = cos(angle) + v[1] * v[1] * (1 - cos(angle));
        res[1][2] = v[0]* sin(angle) + v[1] * v[2] * (1-cos(angle));
        res[1][3] = 0.f;

        res[2][0] = v[1] * sin(angle) + v[0] * v[2] * (1-cos(angle));
        res[2][1] = -v[0] * sin(angle) + v[1] * v[2] * (1-cos(angle));
        res[2][2] = cos(angle) + v[2] * v[2] * (1-cos(angle));
        res[2][3] = 0.f;

        res[3][0] = 0.f;
        res[3][1] = 0.f;
        res[3][2] = 0.f;
        res[3][3] = 1.f;

        return res;
   }

   /// Takes an xyz displacement and outputs a 4x4 translation matrix
    mat4 mat4::translate(float x, float y, float z){
       mat4 m = mat4();
       m[3][0] = x;
       m[3][1] = y;
       m[3][2] = z;

       return m;
   }

   /// Takes an xyz scale and outputs a 4x4 scale matrix
   mat4 mat4::scale(float x, float y, float z){
       mat4 m = mat4();
       m[0][0] = x;
       m[1][1] = y;
       m[2][2] = z;
       return m;

   }

   /// Generates a 4x4 identity matrix
   mat4 mat4::identity(){
       mat4 i = mat4();
       return i;
   }


   ///----------------------------------------------------------------------
   /// Operator Functions
   ///----------------------------------------------------------------------

   /// Assign m2 to this and return it
   mat4& mat4::operator=(const mat4 &m2){
       mat4 res = mat4(m2);
       *this = res;
       return *this;
   }

   /// Test for equality
   bool mat4::operator==(const mat4 &m2) const{
        for(unsigned int i = 0; i<4; i++){
            if(m2[i]!=this->data[i]) return false;
        }
        return true;
   }

   /// Test for inequality
   bool mat4::operator!=(const mat4 &m2) const{
        for(unsigned int i = 0; i<4; i++){
            if(m2[i]!=this->data[i]) return true;
        }
        return false;
   }

   /// Element-wise arithmetic
   /// e.g. += adds the elements of m2 to this and returns this (like regular +=)
   ///      +  returns a new matrix whose elements are the sums of this and v2
   mat4& mat4::operator+=(const mat4 &m2){
       for(unsigned int i =0; i<4 ; i++){
           (*this)[i]+=m2[i];
       }
       return *this;
   }

   mat4& mat4::operator-=(const mat4 &m2){
       for(unsigned int i = 0; i<4; i++){
           (*this)[i]-=m2[i];
       }
       return *this;
   }
   // multiplication by a scalar
   mat4& mat4::operator*=(float c){
       for(unsigned int i = 0; i<4; i++){
           (*this)[i]*=c;
       }
       return *this;

   }
   // division by a scalar
   mat4& mat4::operator/=(float c){
       if(c==0.f) throw std::out_of_range("invalid divisor");
       for(unsigned int i = 0; i<4; i++){
           (*this)[i]/=c;
       }
       return *this;
   }
   mat4  mat4::operator+(const mat4 &m2) const{
       mat4 res = mat4(*this);
       for(unsigned int i = 0; i<4; i++){
           res[i]+=m2[i];
       }
       return res;
   }
   mat4  mat4::operator-(const mat4 &m2) const{
       mat4 res = mat4(*this);
       for(unsigned int i = 0; i<4; i++){
           res[i]-=m2[i];
       }
       return res;
   }
   // multiplication by a scalar
   mat4  mat4::operator*(float c) const{
       mat4 res = mat4(*this);
       for(unsigned int i = 0; i<4; i++){
           res[i]*=c;
       }
       return res;
   }
   // division by a scalar
   mat4  mat4::operator/(float c) const{
       if(c==0.f) throw std::out_of_range("invalid divisor");
        mat4 res = mat4(*this);
       for(unsigned int i = 0; i<4; i++){
           res[i]/=c;
       }
       return *this;
   }

   /// Matrix multiplication (m1 * m2)
   mat4 mat4::operator*(const mat4 &m2) const{
       mat4 res = mat4();
       res[0][0] = res[1][1] = res[2][2] = res[3][3] = 0;
       for(unsigned int i = 0; i<4; i++){
           for(unsigned int j = 0; j<4; j++){
               for(unsigned int k = 0; k<4; k++){
                    res[j][i] += (*this)[k][i] * m2[j][k];
               }
           }
       }
       return res;
   }

   /// Matrix/vector multiplication (m * v)
   /// Assume v is a column vector (ie. a 4x1 matrix)
   vec4 mat4::operator*(const vec4 &v) const{
        vec4 res = vec4();
        for(unsigned int i = 0; i<4; i++){
            for(unsigned int j = 0; j<4; j++){
                res[i]+=(*this)[i][j]*v[j];
            }
        }
        return res;
   }


///----------------------------------------------------------------------
/// Matrix Operations
///----------------------------------------------------------------------
/// Returns the transpose of the input matrix (v_ij == v_ji)
mat4 transpose(const mat4 &m){
    mat4 m_cpy = mat4();
    for(unsigned int i = 0; i<4;i++){
        for(unsigned int j = 0; j<4; j++){
            m_cpy[i][j] = m[j][i];
            m_cpy[j][i] = m[i][j];
        }
    }
    return m_cpy;
}

/// Returns a row of the input matrix
vec4 row(const mat4 &m, unsigned int index){
    vec4 v = vec4();
    for(unsigned int i = 0; i<4;i++){
        v[i]=m[i][index];
    }
    return v;
}

/// Scalar multiplication (c * m)
mat4 operator*(float c, const mat4 &m){
    mat4 res = mat4(m);
    return res *=c;
}

/// Vector/matrix multiplication (v * m)
/// Assume v is a row vector (ie. a 1x4 matrix)
vec4 operator*(const vec4 &v, const mat4 &m){
    vec4 res = vec4();
    for(unsigned int i = 0; i<4; i++){
        res[i]=v[0]*m[i][0]+v[1]*m[i][1]+v[2]*m[i][2]+v[3]*m[i][3];
    }
    return res;
}

/// Prints the matrix to a stream in a nice format
std::ostream &operator<<(std::ostream &o, const mat4 &m){
       for(unsigned int i = 0; i<4; i++){
            for(unsigned int j = 0; j< 4; j++){

               o<<m[j][i] <<" ";
            }
           o<<"\r\n";
       }
      return o;
}
