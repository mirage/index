#include <string.h>
#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/signals.h>
#include <caml/unixsupport.h>

#ifdef _MSC_VER
#include <Windows.h>
#include <BaseTsd.h>
#include <Intsafe.h>
typedef SSIZE_T ssize_t;
typedef HANDLE file_descr_t;
#define File_descr_val Handle_val

ssize_t pread(file_descr_t fd, void *buf, size_t count, size_t offset)
{
  OVERLAPPED oOverlap;
  BOOL bResult;
  DWORD dwError;
  ssize_t ret;
  HRESULT hr;

  /* https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-readfile#synchronization-and-file-position */
  oOverlap.Internal     = 0;
  oOverlap.InternalHigh = 0;
  oOverlap.Offset       = LODWORD(offset);
  oOverlap.OffsetHigh   = HIDWORD(offset);
  oOverlap.hEvent       = 0;
  bResult = ReadFile(fd, buf, count, NULL, &oOverlap);

  if (!bResult) {
    /* ReadFile failed, or is completing asynchrously if opened with
      FILE_FLAG_OVERLAPPED. OCaml does not use FILE_FLAG_OVERLAPPED. */
    dwError = GetLastError();
    if (dwError == ERROR_HANDLE_EOF) {
      /* pread returns 0 on EOF */
      ret = 0;
    } else {
      /* pread returns -1 on any error */
      ret = -1;
    }
  } else {
    /* pread returns number of bytes read */
    hr = ULongPtrToSSIZET(oOverlap.InternalHigh, &ret);
    if(hr != S_OK) {
      /* overflow. too many bytes read */
      ret = -1;
    }
  }
  return ret;
}
#else
typedef size_t file_descr_t;
#define File_descr_val Int_val
#endif

CAMLprim value caml_index_pread_int
(value v_fd, value v_fd_off, value v_buf, value v_buf_off, value v_len)
{
  CAMLparam5(v_fd, v_fd_off, v_buf, v_buf_off, v_len);

  ssize_t ret;
  file_descr_t fd = File_descr_val(v_fd);
  size_t fd_off = Long_val(v_fd_off);
  size_t buf_off = Long_val(v_buf_off);
  size_t len = Long_val(v_len);

  size_t numbytes = (len > UNIX_BUFFER_SIZE) ? UNIX_BUFFER_SIZE : len;
  ret = pread(fd, &Byte(v_buf, buf_off), numbytes, fd_off);

  if (ret == -1) uerror("read", Nothing);

  CAMLreturn(Val_long(ret));
}

CAMLprim value caml_index_pread_int64
(value v_fd, value v_fd_off, value v_buf, value v_buf_off, value v_len)
{
  CAMLparam5(v_fd, v_fd_off, v_buf, v_buf_off, v_len);

  ssize_t ret;
  file_descr_t fd = File_descr_val(v_fd);
  size_t fd_off = Int64_val(v_fd_off);
  size_t buf_off = Long_val(v_buf_off);
  size_t len = Long_val(v_len);

  size_t numbytes = (len > UNIX_BUFFER_SIZE) ? UNIX_BUFFER_SIZE : len;
  ret = pread(fd, &Byte(v_buf, buf_off), numbytes, fd_off);

  if (ret == -1) uerror("read", Nothing);

  CAMLreturn(Val_long(ret));
}
