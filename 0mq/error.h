#ifndef _ERROR_8ADCD2BAEC4D_
#define _ERROR_8ADCD2BAEC4D_

#include <stdexcept>

namespace zmq {

class Error : public std::exception
{
  public:
    Error();

    virtual const char* what() const throw();
    int getErrorCode() const;

  private:
    const int error_;
};

}

#endif /* _ERROR_8ADCD2BAEC4D_ */
