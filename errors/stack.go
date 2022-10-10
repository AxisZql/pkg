package errors

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"runtime"
	"strings"
)

/*
@Author: AxisZql
@Desc: error stack information retrieval method encapsulation
@Date: 2022 Oct 10 4:44 PM
*/

var (
	dunno     = []byte("???") // 未知堆栈
	centerDot = []byte(".")   // 函数层次之间.
	dot       = []byte(".")
)

type StackError interface {
	Error() string
	StackTrace() string
}

//对外暴露接口屏蔽stackError实现细节
type stackError struct {
	err        error
	stackTrace string
}

func (e stackError) Error() string {
	return fmt.Sprintf("%v\n%v", e.err, e.stackTrace)
}

func (e stackError) StackTrace() string {
	return e.stackTrace
}

func StackErrorf(msg string, args ...interface{}) error {
	stack := ""
	for _, arg := range args {
		if stackErr, ok := arg.(stackError); ok {
			// 利用传入堆stackError的堆栈信息进行覆盖
			stack = stackErr.stackTrace
			break
		}
	}
	if stack == "" {
		// 把前5个堆栈信息进行截断，因为前5个堆栈信息是改包产生
		stack = string(Stack(5))
	}
	return stackError{fmt.Errorf(msg, args...), stack}
}

func StackWithoutLF(calldepth int) string {
	traceback := string(Stack(calldepth))
	return strings.Replace(
		strings.Replace(traceback, "\n\t", "[", -1),
		"\n", "]|", -1,
	)
}

// Stack Taken from runtime/debug.go 获取堆栈信息
func Stack(calldepth int) []byte {
	return stack(calldepth)
}

func stack(calldepth int) []byte {
	buf := new(bytes.Buffer)
	var (
		lines    [][]byte
		lastFile string
	)
	for i := calldepth; ; i++ {
		// 输出堆栈地址，对应程序文件名和对应代码所处行号
		// Caller获取的是第i层的信息，而Callers获取的是后i层「包括i」
		pc, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		fmt.Fprintf(buf, "%s:%d(0x%x)\n", file, line, pc)
		// 防止重复读取
		if file != lastFile {
			data, err := ioutil.ReadFile(file)
			if err != nil {
				continue
			}
			lines = bytes.Split(data, []byte{'\n'})
			lastFile = file
		}
		// 因为行号从1开始
		line--
		fmt.Fprintf(buf, "\t%s: %s\n", function(pc), source(lines, line))

	}
	return buf.Bytes()
}

func source(lines [][]byte, n int) []byte {
	if n < 0 || n >= len(lines) {
		return dunno
	}
	// 根据制表符进行分割
	return bytes.Trim(lines[n], "\t")
}

func function(pc uintptr) []byte {
	// 返回对应函数的定义相关信息
	fn := runtime.FuncForPC(pc)
	if fn == nil {
		return dunno
	}
	name := []byte(fn.Name())
	if period := bytes.Index(name, dot); period >= 0 {
		name = name[period+1:]
	}
	name = bytes.Replace(name, centerDot, dot, -1)
	return name
}
