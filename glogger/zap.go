/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package glogger

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zapgrpc"
)

// NewZapLogger creates a zap logger around a new zap.Core. The core will use
// the provided encoder and sinks and a level enabler that is associated with
// the provided module name. The logger that is returned will be named the same
// as the module.
func NewZapLogger(core zapcore.Core, options ...zap.Option) *zap.Logger {
	return zap.New(
		core,
		append([]zap.Option{
			zap.AddCaller(),
			zap.AddStacktrace(zapcore.ErrorLevel),
		}, options...)...,
	)
}

// NewGRPCLogger creates a grpc.Logger that delegates to a zap.Logger.
func NewGRPCLogger(l *zap.Logger) *zapgrpc.Logger {
	l = l.WithOptions(
		zap.AddCaller(),
		zap.AddCallerSkip(3),
	)
	return zapgrpc.NewLogger(l, zapgrpc.WithDebug())
}

// NewFabricLogger creates a logger that delegates to the zap.SugaredLogger.
func NewFabricLogger(l *zap.Logger, options ...zap.Option) *GLogger {
	return &GLogger{
		s: l.WithOptions(append(options, zap.AddCallerSkip(1))...).Sugar(),
	}
}

// A GLogger is an adapter around a zap.SugaredLogger that provides
// structured logging capabilities while preserving much of the legacy logging
// behavior.
//
// The most significant difference between the GLogger and the
// zap.SugaredLogger is that methods without a formatting suffix (f or w) build
// the log entry message with fmt.Sprintln instead of fmt.Sprint. Without this
// change, arguments are not separated by spaces.
type GLogger struct{ s *zap.SugaredLogger }

func (f *GLogger) DPanic(args ...interface{})                    { f.s.DPanicf(formatArgs(args)) }
func (f *GLogger) DPanicf(template string, args ...interface{})  { f.s.DPanicf(template, args...) }
func (f *GLogger) DPanicw(msg string, kvPairs ...interface{})    { f.s.DPanicw(msg, kvPairs...) }
func (f *GLogger) Debug(args ...interface{})                     { f.s.Debugf(formatArgs(args)) }
func (f *GLogger) Debugf(template string, args ...interface{})   { f.s.Debugf(template, args...) }
func (f *GLogger) Debugw(msg string, kvPairs ...interface{})     { f.s.Debugw(msg, kvPairs...) }
func (f *GLogger) Error(args ...interface{})                     { f.s.Errorf(formatArgs(args)) }
func (f *GLogger) Errorf(template string, args ...interface{})   { f.s.Errorf(template, args...) }
func (f *GLogger) Errorw(msg string, kvPairs ...interface{})     { f.s.Errorw(msg, kvPairs...) }
func (f *GLogger) Fatal(args ...interface{})                     { f.s.Fatalf(formatArgs(args)) }
func (f *GLogger) Fatalf(template string, args ...interface{})   { f.s.Fatalf(template, args...) }
func (f *GLogger) Fatalw(msg string, kvPairs ...interface{})     { f.s.Fatalw(msg, kvPairs...) }
func (f *GLogger) Info(args ...interface{})                      { f.s.Infof(formatArgs(args)) }
func (f *GLogger) Infof(template string, args ...interface{})    { f.s.Infof(template, args...) }
func (f *GLogger) Infow(msg string, kvPairs ...interface{})      { f.s.Infow(msg, kvPairs...) }
func (f *GLogger) Panic(args ...interface{})                     { f.s.Panicf(formatArgs(args)) }
func (f *GLogger) Panicf(template string, args ...interface{})   { f.s.Panicf(template, args...) }
func (f *GLogger) Panicw(msg string, kvPairs ...interface{})     { f.s.Panicw(msg, kvPairs...) }
func (f *GLogger) Warn(args ...interface{})                      { f.s.Warnf(formatArgs(args)) }
func (f *GLogger) Warnf(template string, args ...interface{})    { f.s.Warnf(template, args...) }
func (f *GLogger) Warnw(msg string, kvPairs ...interface{})      { f.s.Warnw(msg, kvPairs...) }
func (f *GLogger) Warning(args ...interface{})                   { f.s.Warnf(formatArgs(args)) }
func (f *GLogger) Warningf(template string, args ...interface{}) { f.s.Warnf(template, args...) }

// for backwards compatibility
func (f *GLogger) Critical(args ...interface{})                   { f.s.Errorf(formatArgs(args)) }
func (f *GLogger) Criticalf(template string, args ...interface{}) { f.s.Errorf(template, args...) }
func (f *GLogger) Notice(args ...interface{})                     { f.s.Infof(formatArgs(args)) }
func (f *GLogger) Noticef(template string, args ...interface{})   { f.s.Infof(template, args...) }

func (f *GLogger) Named(name string) *GLogger { return &GLogger{s: f.s.Named(name)} }
func (f *GLogger) Sync() error                { return f.s.Sync() }

func (f *GLogger) IsEnabledFor(level zapcore.Level) bool {
	return f.s.Desugar().Core().Enabled(level)
}

func (f *GLogger) With(args ...interface{}) *GLogger {
	return &GLogger{s: f.s.With(args...)}
}

func (f *GLogger) WithOptions(opts ...zap.Option) *GLogger {
	l := f.s.Desugar().WithOptions(opts...)
	return &GLogger{s: l.Sugar()}
}

func formatArgs(args []interface{}) string { return strings.TrimSuffix(fmt.Sprintln(args...), "\n") }
