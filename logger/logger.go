package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"io"
	"os"
	"path/filepath"
	"time"
)

/*
@Author: AxisZql
@Desc: Encapsulation of zap log library
@Date: 2022 Oct 15 8:07 PM
*/

const (
	DefaultLevel      = zapcore.InfoLevel
	DefaultTimeLayout = time.RFC3339
)

// Option Initialize logger options
type Option func(*option)

// option custom option
type option struct {
	level          zapcore.Level
	fields         map[string]string
	file           io.Writer
	timeLayout     string
	disableConsole bool
}

var Logger *zap.Logger

func WithDebugLevel() Option {
	return func(opt *option) {
		opt.level = zap.DebugLevel
	}
}

func WithInfoLevel() Option {
	return func(opt *option) {
		opt.level = zap.InfoLevel
	}
}

func WithWarnLevel() Option {
	return func(opt *option) {
		opt.level = zap.WarnLevel
	}
}

func WithErrorLevel() Option {
	return func(opt *option) {
		opt.level = zap.ErrorLevel
	}
}

// WithField add some field(s) to log
func WithField(key, value string) Option {
	return func(opt *option) {
		opt.fields[key] = value
	}
}

// WithFileP write log to some file
func WithFileP(file string) Option {
	dir := filepath.Dir(file)
	if err := os.MkdirAll(dir, 0766); err != nil {
		panic(err)
	}
	f, err := os.OpenFile(file, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0766)
	if err != nil {
		panic(err)
	}
	return func(opt *option) {
		opt.file = zapcore.Lock(f) // lock for concurrent safe
	}
}

func WithFileRotationP(file string) Option {
	dir := filepath.Dir(file)
	if err := os.MkdirAll(dir, 0766); err != nil {
		panic(err)
	}
	return func(opt *option) {
		opt.file = &lumberjack.Logger{
			Filename:   file, // file path
			MaxSize:    128,  //Maximum of a single，The default unit is MB
			MaxBackups: 300,  // most have 300 backups
			MaxAge:     30,   // the maximum storage time is 30 days
			LocalTime:  true,
			Compress:   true,
		}
	}
}

func WithTimeLayout(timeLayout string) Option {
	return func(opt *option) {
		opt.timeLayout = timeLayout
	}
}

func WithDisableConsole() Option {
	return func(opt *option) {
		opt.disableConsole = true
	}
}

// InitLogger init the Logger
func InitLogger(opts ...Option) *zap.Logger {
	opt := &option{
		level:      DefaultLevel,
		fields:     make(map[string]string),
		timeLayout: DefaultTimeLayout,
	}
	for _, apply := range opts {
		apply(opt)
	}
	// similar to zap.NewProductionEncoderConfig()
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:       "time",
		LevelKey:      "level",
		NameKey:       "Logger",
		CallerKey:     "caller",
		MessageKey:    "msg",
		StacktraceKey: "stacktrace",
		LineEnding:    zapcore.DefaultLineEnding,
		EncodeLevel:   zapcore.LowercaseLevelEncoder, // 小写编码器
		EncodeTime: func(t time.Time, encoder zapcore.PrimitiveArrayEncoder) {
			encoder.AppendString(t.Format(opt.timeLayout))
		},
		EncodeDuration: zapcore.MillisDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder, // 全路径编码器
	}

	// lowPriority used by info/debug/warn
	lowPriority := zap.LevelEnablerFunc(func(lv zapcore.Level) bool {
		return lv >= opt.level && lv < zapcore.ErrorLevel
	})
	// highPriority used by error/panic/fatal
	highPriority := zap.LevelEnablerFunc(func(lv zapcore.Level) bool {
		return lv >= opt.level && lv >= zapcore.ErrorLevel
	})
	stdout := zapcore.Lock(os.Stdout) //lock for concurrent safe
	stderr := zapcore.Lock(os.Stderr)

	var core zapcore.Core
	encoder := zapcore.NewJSONEncoder(encoderConfig)
	if !opt.disableConsole {
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	}
	core = zapcore.NewTee(
		zapcore.NewCore(
			encoder,
			zapcore.NewMultiWriteSyncer(stdout),
			lowPriority,
		),
		zapcore.NewCore(
			encoder,
			zapcore.NewMultiWriteSyncer(stderr),
			highPriority,
		),
	)
	if opt.file != nil {
		core = zapcore.NewTee(
			core,
			zapcore.NewCore(
				encoder,
				zapcore.AddSync(opt.file),
				zap.LevelEnablerFunc(func(lv zapcore.Level) bool {
					return lv >= opt.level
				}),
			),
		)
	}
	Logger = zap.New(
		core,
		zap.AddCaller(),
		zap.ErrorOutput(stderr),
	)
	for k, v := range opt.fields {
		Logger = Logger.WithOptions(
			zap.Fields(zapcore.Field{Key: k, Type: zapcore.StringerType, String: v}),
		)
	}
	return Logger
}

func GetLogger() *zap.Logger {
	return Logger
}

func setLogger() {
	if Logger == nil {
		Logger, _ = zap.NewProduction(zap.AddStacktrace(zapcore.LevelEnabler(zapcore.ErrorLevel)))
	}
}

func Info(msg string, fields ...zap.Field) {
	setLogger()
	Logger.Info(msg, fields...)
}

func Debug(msg string, fields ...zap.Field) {
	setLogger()
	Logger.Debug(msg, fields...)
}

func Warn(msg string, fields ...zap.Field) {
	setLogger()
	Logger.Warn(msg, fields...)
}
func Error(msg string, fields ...zap.Field) {
	setLogger()
	Logger.Error(msg, fields...)
}

func Sync() {
	Logger.Sync()
}

// Meta key-value
type Meta interface {
	Key() string
	Value() interface{}
	meta()
}

type meta struct {
	key   string
	value interface{}
}

func (m *meta) Key() string {
	return m.key
}

func (m *meta) Value() interface{} {
	return m.value
}

func (m *meta) meta() {}

func NewMeta(key string, value interface{}) Meta {
	return &meta{key: key, value: value}
}

func WrapMeta(err error, metas ...Meta) (fields []zap.Field) {
	capacity := len(metas) + 1
	if err != nil {
		capacity++
	}
	fields = make([]zap.Field, 0, capacity)
	if err != nil {
		fields = append(fields, zap.Error(err))
	}
	fields = append(fields, zap.Namespace("meta"))
	for _, meta := range metas {
		fields = append(fields, zap.Any(meta.Key(), meta.Value()))
	}
	return
}
