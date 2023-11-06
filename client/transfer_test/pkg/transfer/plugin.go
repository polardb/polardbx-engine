package transfer

import (
	"context"

	"transfer/pkg/ssot"
)

type basePlugin struct {
	connector   Connector
	tso         TSO
	sourceTruth ssot.Source
	conf        *Config
	globals     *Globals
	metrics     *Metrics
	sessionHint *SessionHint
}

// PluginBuilder is used to build every plugin.
// Every Plugin should register a Build{plugin_name} method on PluginBuilder.
type PluginBuilder struct {
	basePlugin
}

func (b PluginBuilder) Connector(c Connector) PluginBuilder {
	b.connector = c
	return b
}

func (b PluginBuilder) TSO(tso TSO) PluginBuilder {
	b.tso = tso
	return b
}

func (b PluginBuilder) Conf(conf *Config) PluginBuilder {
	b.conf = conf
	return b
}

func (b PluginBuilder) Globals(globals *Globals) PluginBuilder {
	b.globals = globals
	return b
}

func (b PluginBuilder) Metrics(metrics *Metrics) PluginBuilder {
	b.metrics = metrics
	return b
}

func (b PluginBuilder) Ssot(source ssot.Source) PluginBuilder {
	b.sourceTruth = source
	return b
}

func (b PluginBuilder) SessionHint(sessionHint *SessionHint) PluginBuilder {
	b.sessionHint = sessionHint
	return b
}

type Plugin interface {
	Round(ctx context.Context, id string) error
	Name() string
}
