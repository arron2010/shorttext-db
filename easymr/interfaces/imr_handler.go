package interfaces

/*
注册gob序列化与反序列化类型
*/
type IRegisterForSerialization interface {
	RegisterTypes()
}
