import struct

from bluepy.btle import Peripheral

address = "5C:F2:86:40:D0:42"
address2 = "5C:F2:86:40:46:1A"
address3 = "5c:f2:86:40:d0:3f"
address4 = "5c:f2:86:40:d0:3c"
address5 = "5c:f2:86:40:d0:33"

USENSOR_UART_UUID = "0000FFF0-0000-1000-8000-00805f9b34fb"
UUID_NORDIC_TX = "0000fff2-0000-1000-8000-00805f9b34fb"
UUID_NORDIC_RX = "0000fff1-0000-1000-8000-00805f9b34fb"

ConfigHndl = 0x000e
ConfigHnd2 = 0x0010
ConfigHnd3 = 0x00d



try:
    per = Peripheral()
    per.connect(address)
    print("connected")

    per.writeCharacteristic(ConfigHndl, bytearray.fromhex('01 00'))
    while True:
        data = per.readCharacteristic(ConfigHnd3)
        per.writeCharacteristic(ConfigHnd2, bytearray.fromhex('fd 02 00 05 ff'))

        print({
            'len': len(data),
            'data': [hex(x) for x in data],
        })

        if data[0] == 0xfd:
            # Request Sending Interval
            if data[1] == 0x00 and data[2] == 0xff:
                def set_interval_secs(secs):
                    return per.writeCharacteristic(
                        ConfigHnd2,
                        bytearray.fromhex(f'fd 02 00 {secs:02x} ff'),
                    )

                set_interval_secs(3)

            # Timing Interval ACK
            if data[1] == 0x02 and data[4] == 0xfe:
                per.writeCharacteristic(ConfigHnd2, b"\xfd\x00\xfd")

        if data[0] == 0xf9:
            hexdata = [int(x) for x in data]
            hexdata1 = hexdata[2:6]
            hexdata2 = hexdata[6:10]
            hexdata1 = bytearray(hexdata1)
            hexdata2 = bytearray(hexdata2)
            hexdata1 = struct.unpack('<f', hexdata1)
            hexdata2 = struct.unpack('<f', hexdata2)

            print(hexdata1[0])
            print(hexdata2[0])

finally:
    per.disconnect()
