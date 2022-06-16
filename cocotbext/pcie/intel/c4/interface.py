"""

Copyright (c) 2022 Yaotian Feng

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

"""

import logging
import struct
from abc import ABC

import cocotb
from cocotb.queue import Queue, QueueFull
from cocotb.triggers import RisingEdge, Timer, First, Event
from cocotb_bus.bus import Bus

from cocotbext.pcie.core.tlp import Tlp, TlpType


class BaseBus(Bus):
    _signals = ["data"]
    _optional_signals = []

    def __init__(self, entity=None, prefix=None, **kwargs):
        super().__init__(entity, prefix, self._signals, optional_signals=self._optional_signals, **kwargs)

    @classmethod
    def from_entity(cls, entity, **kwargs):
        return cls(entity, **kwargs)

    @classmethod
    def from_prefix(cls, entity, prefix, **kwargs):
        return cls(entity, prefix, **kwargs)


class C4TxBus(BaseBus):
    _signals = ["data", "sop", "eop", "valid", "ready", "err"]
    _optional_signals = []


class C4DecoupledTxBus(C4TxBus):
    _signals = {
        signal: signal if signal in {'valid', 'ready'} else f'bits_{signal}' for signal in C4TxBus._signals
    }
    _optional_signals = {
        signal: f'bits_{signal}' for signal in C4TxBus._optional_signals
    }


class C4RxBus(BaseBus):
    _signals = ["valid", "ready", "data", "sop", "eop", "err", "be", "bardec"]
    _optional_signals = []


class C4DecoupledRxBus(C4RxBus):
    _signals = {
        signal: signal if signal in {'valid', 'ready'} else f'bits_{signal}' for signal in C4RxBus._signals
    }
    _optional_signals = {
        signal: f'bits_{signal}' for signal in C4RxBus._optional_signals
    }


class C4PcieFrame:
    def __init__(self, frame=None):
        self.data = []
        self.bardec = 0b00000001
        self.err = 0

        if isinstance(frame, Tlp):
            hdr = frame.pack_header()
            for k in range(0, len(hdr), 4):
                self.data.extend(struct.unpack_from('>L', hdr, k))

            is_qword_aligned = (frame.get_address_dw() & 1) == 0
            insert_alignment_dw = (frame.get_header_size_dw() == 3 and is_qword_aligned) or \
                                  (frame.get_header_size_dw() == 4 and not is_qword_aligned)
            if insert_alignment_dw:
                self.data.extend([0])

            data = frame.get_data()
            for k in range(0, len(data), 4):
                self.data.extend(struct.unpack_from('<L', data, k))
        elif isinstance(frame, C4PcieFrame):
            self.data = list(frame.data)
            self.bardec = frame.bardec
            self.err = frame.err

    @classmethod
    def from_tlp(cls, tlp):
        return cls(tlp)

    def to_tlp(self):
        hdr = bytearray()
        for dw in self.data[:5]:
            hdr.extend(struct.pack('>L', dw))
        tlp = Tlp.unpack_header(hdr)

        alignment_padding = 0
        if tlp.has_data():
            if tlp.fmt_type in {TlpType.CPL_DATA, TlpType.CPL_LOCKED_DATA}:
                is_qword_aligned = (tlp.lower_address & 0b100) == 0
            else:
                is_qword_aligned = (tlp.get_address_dw() & 1) == 0
            insert_alignment_dw = (tlp.get_header_size_dw() == 3 and is_qword_aligned) or \
                                  (tlp.get_header_size_dw() == 4 and not is_qword_aligned)
            if insert_alignment_dw:
                alignment_padding = 1

        for dw in self.data[tlp.get_header_size_dw()+alignment_padding:]:
            tlp.data.extend(struct.pack('<L', dw))

        return tlp

    def update_parity(self):
        raise NotImplementedError()

    def check_parity(self):
        raise NotImplementedError()

    def __eq__(self, other):
        if isinstance(other, C4PcieFrame):
            return (
                    self.data == other.data and
                    self.bardec == other.bardec and
                    self.err == other.err
            )
        return False

    def __repr__(self):
        return (
            f"{type(self).__name__}(data=[{', '.join(f'{x:#010x}' for x in self.data)}], "
            f"bardec={self.bardec}, "
            f"err={self.err})"
        )

    def __len__(self):
        return len(self.data)


class C4PcieTransaction:

    _signals = ["valid", "data", "sop", "eop", "err", "be", "bardec"]

    def __init__(self, *args, **kwargs):
        for sig in self._signals:
            if sig in kwargs:
                setattr(self, sig, kwargs[sig])
                del kwargs[sig]
            else:
                setattr(self, sig, 0)

        super().__init__(*args, **kwargs)

    def __repr__(self):
        return f"{type(self).__name__}({', '.join(f'{s}={int(getattr(self, s))}' for s in self._signals)})"


class C4PcieBase:
    _signal_widths = {"ready": 1}

    _valid_signal = "valid"
    _ready_signal = "ready"

    _transaction_obj = C4PcieTransaction
    _frame_obj = C4PcieFrame

    def __init__(self, bus, clock, reset=None, ready_latency=0, bus_payload_prefix=None, *args, **kwargs):
        self.bus = bus
        self.clock = clock
        self.reset = reset
        self.ready_latency = ready_latency
        self.log = logging.getLogger(f"cocotb.{bus._entity._name}.{bus._name}")

        super().__init__(*args, **kwargs)

        self.active = False
        self.queue = Queue()
        self.dequeue_event = Event()
        self.idle_event = Event()
        self.idle_event.set()
        self.active_event = Event()

        self.pause = False
        self._pause_generator = None
        self._pause_cr = None

        self.queue_occupancy_bytes = 0
        self.queue_occupancy_frames = 0

        self.width = len(self.bus.data)
        self.byte_size = 32
        self.byte_lanes = self.width // self.byte_size
        self.byte_mask = 2 ** self.byte_size - 1

        assert self.width in {64, 128}

    def count(self):
        return self.queue.qsize()

    def empty(self):
        return self.queue.empty()

    def clear(self):
        while not self.queue.empty():
            self.queue.get_nowait()
        self.idle_event.set()
        self.active_event.clear()

    def idle(self):
        raise NotImplementedError()

    async def wait(self):
        raise NotImplementedError()


class C4PcieSource(C4PcieBase):
    _signal_widths = {"valid": 1, "ready": 1}

    _valid_signal = "valid"
    _ready_signal = "ready"

    _transaction_obj = C4PcieTransaction
    _frame_obj = C4PcieFrame

    def __init__(self, bus, clock, reset=None, ready_latency=0, *args, **kwargs):
        super().__init__(bus, clock, reset, ready_latency, *args, **kwargs)

        self.drive_obj = None
        self.drive_sync = Event()

        self.queue_occupancy_limit_bytes = -1
        self.queue_occupancy_limit_frames = -1

        self.bus.data.setimmediatevalue(0)
        self.bus.sop.setimmediatevalue(0)
        self.bus.eop.setimmediatevalue(0)
        self.bus.valid.setimmediatevalue(0)
        self.bus.err.setimmediatevalue(0)
        self.bus.be.setimmediatevalue(0)
        self.bus.bardec.setimmediatevalue(0)

        cocotb.start_soon(self._run_source())
        cocotb.start_soon(self._run())

    async def _drive(self, obj):
        if self.drive_obj is not None:
            self.drive_sync.clear()
            await self.drive_sync.wait()

        self.drive_obj = obj

    async def send(self, frame):
        while self.full():
            self.dequeue_event.clear()
            await self.dequeue_event.wait()
        frame = C4PcieFrame(frame)
        await self.queue.put(frame)
        self.idle_event.clear()
        self.queue_occupancy_bytes += len(frame)
        self.queue_occupancy_frames += 1

    def send_nowait(self, frame):
        if self.full():
            raise QueueFull()
        frame = C4PcieFrame(frame)
        self.queue.put_nowait(frame)
        self.idle_event.clear()
        self.queue_occupancy_bytes += len(frame)
        self.queue_occupancy_frames += 1

    def full(self):
        if 0 < self.queue_occupancy_limit_bytes < self.queue_occupancy_bytes:
            return True
        elif 0 < self.queue_occupancy_limit_frames < self.queue_occupancy_frames:
            return True
        else:
            return False

    def idle(self):
        return self.empty() and not self.active

    async def wait(self):
        await self.idle_event.wait()

    async def _run_source(self):
        self.active = False
        ready_delay = []

        clock_edge_event = RisingEdge(self.clock)

        while True:
            await clock_edge_event

            # read handshake signals
            ready_sample = self.bus.ready.value
            valid_sample = self.bus.valid.value

            if self.reset is not None and self.reset.value:
                self.active = False
                self.bus.valid.value = 0
                continue

            # ready delay
            if self.ready_latency > 1:
                if len(ready_delay) != (self.ready_latency-1):
                    ready_delay = [0]*(self.ready_latency-1)
                ready_delay.append(ready_sample)
                ready_sample = ready_delay.pop(0)

            if (ready_sample and valid_sample) or not valid_sample or self.ready_latency > 0:
                if self.drive_obj and not self.pause and (ready_sample or self.ready_latency == 0):
                    self.bus.drive(self.drive_obj)
                    self.drive_obj = None
                    self.drive_sync.set()
                    self.active = True
                else:
                    self.bus.valid.value = 0
                    self.active = bool(self.drive_obj)
                    if not self.drive_obj:
                        self.idle_event.set()

    async def _run(self):
        while True:
            frame = await self._get_frame()
            frame_offset = 0
            self.log.info("TX frame: %r", frame)
            first = True

            frame: C4PcieFrame
            while frame is not None:
                transaction = self._transaction_obj()

                if frame is None:
                    if not self.empty():
                        frame = self._get_frame_nowait()
                        frame_offset = 0
                        self.log.info("TX frame: %r", frame)
                        first = True
                    else:
                        break

                if first:
                    first = False

                    transaction.valid = 1
                    transaction.sop = 1

                if self.width == 128 and frame_offset == 0:
                    transaction.bardec = frame.bardec
                elif self.width == 64 and frame_offset == 2:
                    transaction.bardec = frame.bardec
                else:
                    transaction.bardec = 0

                transaction.err = frame.err

                if frame.data:
                    transaction.valid = 1

                    for k in range(min(self.byte_lanes, len(frame.data)-frame_offset)):
                        transaction.data |= frame.data[frame_offset] << 32*k
                        frame_offset += 1

                if frame_offset >= len(frame.data):
                    transaction.eop = 1
                    frame = None

                await self._drive(transaction)

    async def _get_frame(self):
        frame = await self.queue.get()
        self.dequeue_event.set()
        self.queue_occupancy_bytes -= len(frame)
        self.queue_occupancy_frames -= 1
        return frame

    def _get_frame_nowait(self):
        frame = self.queue.get_nowait()
        self.dequeue_event.set()
        self.queue_occupancy_bytes -= len(frame)
        self.queue_occupancy_frames -= 1
        return frame


class C4PcieSink(C4PcieBase):

    _signal_widths = {"valid": 1, "ready": 1}

    _valid_signal = "valid"
    _ready_signal = "ready"

    _transaction_obj = C4PcieTransaction
    _frame_obj = C4PcieFrame

    def __init__(self, bus, clock, reset=None, ready_latency=0, *args, **kwargs):
        super().__init__(bus, clock, reset, ready_latency, *args, **kwargs)

        self.sample_obj = None
        self.sample_sync = Event()

        self.queue_occupancy_limit_bytes = -1
        self.queue_occupancy_limit_frames = -1

        self.bus.ready.setimmediatevalue(0)

        cocotb.start_soon(self._run_sink())
        cocotb.start_soon(self._run())

    def _recv(self, frame):
        if self.queue.empty():
            self.active_event.clear()
        self.queue_occupancy_bytes -= len(frame)
        self.queue_occupancy_frames -= 1
        return frame

    async def recv(self):
        frame = await self.queue.get()
        return self._recv(frame)

    def recv_nowait(self):
        frame = self.queue.get_nowait()
        return self._recv(frame)

    def full(self):
        if 0 < self.queue_occupancy_limit_bytes < self.queue_occupancy_bytes:
            return True
        elif 0 < self.queue_occupancy_limit_frames < self.queue_occupancy_frames:
            return True
        else:
            return False

    def idle(self):
        return not self.active

    async def wait(self, timeout=0, timeout_unit='ns'):
        if not self.empty():
            return
        if timeout:
            await First(self.active_event.wait(), Timer(timeout, timeout_unit))
        else:
            await self.active_event.wait()

    async def _run_sink(self):
        ready_delay = []

        clock_edge_event = RisingEdge(self.clock)

        while True:
            await clock_edge_event

            # read handshake signals
            ready_sample = self.bus.ready.value
            valid_sample = self.bus.valid.value

            if self.reset is not None and self.reset.value:
                self.bus.ready.value = 0
                continue

            # ready delay
            if self.ready_latency > 0:
                if len(ready_delay) != self.ready_latency:
                    ready_delay = [0]*self.ready_latency
                ready_delay.append(ready_sample)
                ready_sample = ready_delay.pop(0)

            if valid_sample and ready_sample:
                self.sample_obj = self._transaction_obj()
                self.bus.sample(self.sample_obj)
                self.sample_sync.set()
            elif self.ready_latency > 0:
                assert not valid_sample, "handshake error: valid asserted outside of ready cycle"

            self.bus.ready.value = (not self.full() and not self.pause)

    async def _run(self):
        self.active = False
        frame = None
        hdr_buffer = None
        dword_count = 0

        while True:
            while not self.sample_obj:
                self.sample_sync.clear()
                await self.sample_sync.wait()

            self.active = True
            sample = self.sample_obj
            self.sample_obj = None

            if not sample.valid & 1:
                continue

            if sample.sop & 1:
                assert frame is None, "framing error: sop asserted in frame"
                frame = C4PcieFrame()
                hdr_buffer = sample.data
                frame.err = sample.err
                dword_count = 2
            elif hdr_buffer is not None:    # Second half of header
                assert sample.sop == 0, "SOP on second frame"
                hdr = (sample.data << 64) | hdr_buffer
                hdr_buffer = None
                fmt = (hdr >> 29) & 0b111
                is4dw = (fmt & 0b001) != 0
                if is4dw:
                    dword_count = 2
                else:
                    dword_count = 1

                if fmt & 0b010:  # has data
                    count = hdr & 0x3ff
                    if count == 0:
                        count = 1024
                    qw_aligned = ((hdr >> 64) & 0b100) == 0 if not is4dw else ((hdr >> 96) & 0b100) == 0
                    if (not is4dw and qw_aligned) or (is4dw and not qw_aligned):
                        dword_count += 1
                    dword_count += count

                frame.err |= sample.err

            assert frame is not None, "framing error: data transferred outside of frame"

            if dword_count > 0:
                data = sample.data
                for k in range(min(self.byte_lanes, dword_count)):
                    frame.data.append((data >> 32*k) & 0xffffffff)
                    dword_count -= 1

            if sample.eop:
                assert dword_count == 0, "framing error: incorrect length or early eop"
                self.log.info("RX frame: %r", frame)
                self._sink_frame(frame)
                self.active = False
                frame = None

    def _sink_frame(self, frame):
        self.queue_occupancy_bytes += len(frame)
        self.queue_occupancy_frames += 1

        self.queue.put_nowait(frame)
        self.active_event.set()
