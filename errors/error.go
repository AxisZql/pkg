package errors

import (
	"fmt"
	"github.com/pkg/errors"
	"io"
	"log"
	"runtime"
)

/*
@Author: AxisZql
@Desc: github.com/pkg/errors encapsulation
@Date: 2022 Oct 10 17:15 PM
*/

func callers() []uintptr {
	var pcs [32]uintptr
	// 0 标识Callers本身的帧，1标识Callers的调用者，
	// 2则标识调用callers的对象，3则标识应用的error入口
	l := runtime.Callers(3, pcs[:])
	return pcs[:l]
}

// Error an error with caller stack information
type Error interface {
	error
}

type item struct {
	msg   string
	stack []uintptr
}

func (i *item) Error() string {
	return i.msg
}

// Format used by go.uber.org/zap in Verbose
func (i *item) Format(s fmt.State, verb rune) {
	io.WriteString(s, i.msg)
	io.WriteString(s, "\n")
	for _, pc := range i.stack {
		// 打印堆栈
		fmt.Fprintf(s, "%+v\n", errors.Frame(pc))
	}
}

func New(msg string) Error {
	return &item{msg: msg, stack: callers()}
}

func Errorf(format string, args ...interface{}) Error {
	return &item{msg: fmt.Sprintf(format, args...), stack: callers()}
}

// Wrap with some extra message into err
func Wrap(err error, msg string) Error {
	if err == nil {
		return nil
	}
	e, ok := err.(*item)
	if !ok {
		return &item{msg: fmt.Sprintf("%s;%s", msg, err.Error()), stack: callers()}
	}
	e.msg = fmt.Sprintf("%s;%s", msg, err.Error())
	return e
}

func Wrapf(err error, format string, args ...interface{}) Error {
	if err == nil {
		return nil
	}
	msg := fmt.Sprintf(format, args...)
	e, ok := err.(*item)
	if !ok {
		return &item{msg: fmt.Sprintf("%s;%s", msg, err.Error()), stack: callers()}
	}
	e.msg = fmt.Sprintf("%s;%s", msg, err.Error())
	return e
}

func WithStack(err error) Error {
	if err == nil {
		return nil
	}
	if e, ok := err.(*item); ok {
		return e
	}
	return &item{msg: err.Error(), stack: callers()}
}

func Recover() {
	e := recover()
	if e != nil {
		s := Stack(2)
		log.Fatalf("Panic: %v\nTraceback\r:%s", e, string(s))
	}
}

func RecoverStackWithoutLF() {
	e := recover()
	if e != nil {
		s := StackWithoutLF(3)
		log.Fatalf("Panic: %v\nTraceback\r:%s", e, s)
	}
}
