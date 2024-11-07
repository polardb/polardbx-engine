/*****************************************************************************

Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/


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
