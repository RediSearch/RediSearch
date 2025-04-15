use std::io;
use std::io::Cursor;

use std::io::Seek;
use std::io::Write;
use std::io::SeekFrom;

fn qint_encode4(cursor: &mut Cursor<&mut [u8]>, a: u32, b: u32, c: u32, d: u32) -> Result<usize, io::Error> {
    let mut leading = 0;
    let mut ret = 0;
    let pos = cursor.position() as i64;
    ret += cursor.write(&['\0' as u8])?;
    ret += qint_encode_stepwise(&mut leading, cursor, a, 0)?;
    ret += qint_encode_stepwise(&mut leading, cursor, b, 1)?;
    ret += qint_encode_stepwise(&mut leading, cursor, c, 2)?;
    ret += qint_encode_stepwise(&mut leading, cursor, d, 3)?;
    cursor.seek(SeekFrom::Current(-pos))?;
    cursor.write(&[leading])?;
    Ok(ret)
}

fn qint_encode3(cursor: &mut Cursor<&mut [u8]>, a: u32, b: u32, c: u32) -> Result<usize, io::Error>{
    let mut leading = 0;
    let pos = cursor.position() as i64;
    let mut ret = 0;
    ret += cursor.write(&['\0' as u8])?;
    ret += qint_encode_stepwise(&mut leading, cursor, a, 0)?;
    ret += qint_encode_stepwise(&mut leading, cursor, b, 1)?;
    ret += qint_encode_stepwise(&mut leading, cursor, c, 2)?;
    cursor.seek(SeekFrom::Current(-pos))?;
    cursor.write(&[leading])?;
    Ok(ret)
}

fn qint_encode2(cursor: &mut Cursor<&mut [u8]>, a: u32, b: u32) -> Result<usize, io::Error> {
    let mut leading = 0;
    let pos = cursor.position() as i64;
    let mut ret = 0;
    ret += cursor.write(&['\0' as u8])?;
    ret += qint_encode_stepwise(&mut leading, cursor, a, 0)?;
    ret += qint_encode_stepwise(&mut leading, cursor, b, 1)?;
    cursor.seek(SeekFrom::Current(-pos))?;
    cursor.write(&[leading])?;
    Ok(ret)
}

/// Encodes one byte of using qint encoding. 
#[inline(always)]
fn qint_encode_stepwise(leading: &mut u8, cursor: &mut Cursor<&mut [u8]>, mut value: u32, offset: u32) -> Result<usize, io::Error> {
    let mut ret: usize = 0;
    let mut num_bytes: i8 = -1;
    loop {
        ret += cursor.write(&[value as u8])?;
        num_bytes += 1;

        // shift right until we have no more bigger bytes that are non zero
        value = value >> 8;
        // do while(value) in c
        if value == 0 {
            break;
        }
    }
    // encode the bit length of our integer into the leading byte.
    // 0 means 1 byte, 1 - 2 bytes, 2 - 3 bytes, 3 - 4 bytes.
    // we encode it at the i*2th place in the leading byte
    *leading |= (num_bytes as u8) << (offset * 2);
    Ok(ret)
}

fn qint_decode4(r:  &mut Cursor<&mut [u8]>, a: &mut u32, b: &mut u32, c: &mut u32, d: &mut u32) -> Result<usize, io::Error> {
    todo!()
}

fn qint_decode3(r:  &mut Cursor<&mut [u8]>, a: &mut u32, b: &mut u32, c: &mut u32) -> Result<usize, io::Error> {
    todo!()
}

fn qint_decode2(r:  &mut Cursor<&mut [u8]>, a: &mut u32, b: &mut u32) -> Result<usize, io::Error> {
    todo!()
}

fn qint_decode_stepwise(value: &mut u32, offset: u32, ) -> Result<usize, io::Error> {
    todo!()
}

#[cfg(test)]
mod test {
    /*
    int main(int argc, char **argv) {
        RMUTil_InitAlloc();
        Buffer b = {0};
        Buffer_Init(&b, 1024);
        BufferWriter w = NewBufferWriter(&b);
        qint_encode4(&w, 123, 456, 789, 101112);

        uint32_t arr[4];
        BufferReader r = NewBufferReader(&b);
        qint_decode4(&r, &arr[0], &arr[1], &arr[2], &arr[3]);
        assert(arr[0] == 123);
        assert(arr[1] == 456);
        assert(arr[2] == 789);
        assert(arr[3] == 101112);

        memset(arr, 0, sizeof arr);
        r = NewBufferReader(&b);
        qint_decode3(&r, &arr[0], &arr[1], &arr[2]);
        assert(arr[0] == 123);
        assert(arr[1] == 456);
        assert(arr[2] == 789);
        Buffer_Free(&b);
        return 0;
      }
      */
}
