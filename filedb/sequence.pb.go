// Code generated by protoc-gen-go. DO NOT EDIT.
// source: sequence.proto

package filedb

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type SequenceMsg struct {
	Start                uint64   `protobuf:"varint,1,opt,name=Start,proto3" json:"Start,omitempty"`
	Next                 uint64   `protobuf:"varint,2,opt,name=Next,proto3" json:"Next,omitempty"`
	Name                 string   `protobuf:"bytes,3,opt,name=Name,proto3" json:"Name,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *SequenceMsg) Reset()         { *m = SequenceMsg{} }
func (m *SequenceMsg) String() string { return proto.CompactTextString(m) }
func (*SequenceMsg) ProtoMessage()    {}
func (*SequenceMsg) Descriptor() ([]byte, []int) {
	return fileDescriptor_e97b888ecada2421, []int{0}
}

func (m *SequenceMsg) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SequenceMsg.Unmarshal(m, b)
}
func (m *SequenceMsg) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SequenceMsg.Marshal(b, m, deterministic)
}
func (m *SequenceMsg) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SequenceMsg.Merge(m, src)
}
func (m *SequenceMsg) XXX_Size() int {
	return xxx_messageInfo_SequenceMsg.Size(m)
}
func (m *SequenceMsg) XXX_DiscardUnknown() {
	xxx_messageInfo_SequenceMsg.DiscardUnknown(m)
}

var xxx_messageInfo_SequenceMsg proto.InternalMessageInfo

func (m *SequenceMsg) GetStart() uint64 {
	if m != nil {
		return m.Start
	}
	return 0
}

func (m *SequenceMsg) GetNext() uint64 {
	if m != nil {
		return m.Next
	}
	return 0
}

func (m *SequenceMsg) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func init() {
	proto.RegisterType((*SequenceMsg)(nil), "filedb.SequenceMsg")
}

func init() { proto.RegisterFile("sequence.proto", fileDescriptor_e97b888ecada2421) }

var fileDescriptor_e97b888ecada2421 = []byte{
	// 108 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x2b, 0x4e, 0x2d, 0x2c,
	0x4d, 0xcd, 0x4b, 0x4e, 0xd5, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0x4b, 0xcb, 0xcc, 0x49,
	0x4d, 0x49, 0x52, 0xf2, 0xe6, 0xe2, 0x0e, 0x86, 0xca, 0xf8, 0x16, 0xa7, 0x0b, 0x89, 0x70, 0xb1,
	0x06, 0x97, 0x24, 0x16, 0x95, 0x48, 0x30, 0x2a, 0x30, 0x6a, 0xb0, 0x04, 0x41, 0x38, 0x42, 0x42,
	0x5c, 0x2c, 0x7e, 0xa9, 0x15, 0x25, 0x12, 0x4c, 0x60, 0x41, 0x30, 0x1b, 0x2c, 0x96, 0x98, 0x9b,
	0x2a, 0xc1, 0xac, 0xc0, 0xa8, 0xc1, 0x19, 0x04, 0x66, 0x27, 0xb1, 0x81, 0xcd, 0x36, 0x06, 0x04,
	0x00, 0x00, 0xff, 0xff, 0x45, 0x09, 0x1c, 0xfa, 0x6d, 0x00, 0x00, 0x00,
}
