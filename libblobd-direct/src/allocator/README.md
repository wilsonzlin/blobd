# Allocator

## Bitmaps

### Initial device layout

Assuming a spage size of 512 bytes and lpage size of 8192 bytes (5 page sizes):

||Heap offset 0|2560|5120|…|
|---|---|---|---|---|
|+ 0|Bitmap container for pages 0–4032 of size 512|Pages 4032–8064 of size 512|Pages 8064–12096 of size 512||
|+ 512|Pages 0–4032 of size 1024|Pages 4032–8064 of size 1024|Pages 8064–12096 of size 1024||
|+ 1024|Pages 0–4032 of size 2048|Pages 4032–8064 of size 2048|Pages 8064–12096 of size 2048||
|+ 1536|Pages 0–4032 of size 4096|Pages 4032–8064 of size 4096|Pages 8064–12096 of size 4096||
|+ 2048|Pages 0–4032 of size 8192|Pages 4032–8064 of size 8192|Pages 8064–12096 of size 8192||

### Container layout

<table style="border: 1px solid gray; border-collapse: collapse">
  <tbody>
    <tr style="height: 200px; border: 0">
      <td colSpan="2" style="text-align: center">Bits representing free pages</td>
    </tr>
    <tr>
      <td style="width: 260px; border: 0"></td>
      <td style="border: 1px solid gray; text-align: center">Device offset of<br>next container (u64)</td>
    </tr>
  </tbody>
</table>
