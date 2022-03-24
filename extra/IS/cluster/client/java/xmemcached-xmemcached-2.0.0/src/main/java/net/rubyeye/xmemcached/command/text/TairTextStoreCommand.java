/**
 *Copyright [2009-2010] [dennis zhuang(killme2008@gmail.com)]
 *Licensed under the Apache License, Version 2.0 (the "License");
 *you may not use this file except in compliance with the License. 
 *You may obtain a copy of the License at 
 *             http://www.apache.org/licenses/LICENSE-2.0 
 *Unless required by applicable law or agreed to in writing, 
 *software distributed under the License is distributed on an "AS IS" BASIS, 
 *WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 *either express or implied. See the License for the specific language governing permissions and limitations under the License
 */
/**
 *Copyright [2009-2010] [dennis zhuang(killme2008@gmail.com)]
 *Licensed under the Apache License, Version 2.0 (the "License");
 *you may not use this file except in compliance with the License.
 *You may obtain a copy of the License at
 *             http://www.apache.org/licenses/LICENSE-2.0
 *Unless required by applicable law or agreed to in writing,
 *software distributed under the License is distributed on an "AS IS" BASIS,
 *WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *either express or implied. See the License for the specific language governing permissions and limitations under the License
 */
package net.rubyeye.xmemcached.command.text;

import com.google.code.yanf4j.buffer.IoBuffer;
import net.rubyeye.xmemcached.command.Command;
import net.rubyeye.xmemcached.command.CommandType;
import net.rubyeye.xmemcached.command.StoreCommand;
import net.rubyeye.xmemcached.impl.MemcachedTCPSession;
import net.rubyeye.xmemcached.monitor.Constants;
import net.rubyeye.xmemcached.transcoders.CachedData;
import net.rubyeye.xmemcached.transcoders.Transcoder;
import net.rubyeye.xmemcached.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

/**
 * Store command for text protocol
 * 
 * @author dennis
 * 
 */
public class TairTextStoreCommand extends TextStoreCommand implements StoreCommand{
	protected int namespace;
	protected int version;

	@SuppressWarnings("unchecked")
	public TairTextStoreCommand(String key, byte[] keyBytes, CommandType cmdType,
								CountDownLatch latch, int exp, long cas, Object value,
								boolean noreply, int namespace, int version, Transcoder transcoder) {
		super(key, keyBytes, cmdType, latch, exp, cas, value, noreply, transcoder);
		this.namespace = namespace;
		this.version = version;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public int getNamespace() {
		return namespace;
	}

	public void setNamespace(int namespace) {
		this.namespace = namespace;
	}

	@Override
	public void encode() {
		final CachedData data = encodeValue();
		String cmdStr = getCommandName();
		byte[] encodedData = data.getData();
		int flag = data.getFlag();
		int size = cmdStr.length() + this.keyBytes.length
				+ ByteUtils.stringSize(flag)
				+ ByteUtils.stringSize(this.expTime) + encodedData.length
                + ByteUtils.stringSize(this.namespace)
                + ByteUtils.stringSize(this.version)
				+ ByteUtils.stringSize(encodedData.length) + 10;
		if (this.commandType == CommandType.CAS) {
			size += 1 + ByteUtils.stringSize(this.cas);
		}
		byte[] buf;
		if (isNoreply()) {
			buf = new byte[size + 8];
		} else {
			buf = new byte[size];
		}
		int offset = 0;
		if (this.commandType == CommandType.CAS) {
			if (isNoreply()) {
				offset = ByteUtils.setArguments(buf, offset, cmdStr,
						this.keyBytes, flag, this.expTime, encodedData.length,
						this.cas, Constants.NO_REPLY);
			} else {
				offset = ByteUtils.setArguments(buf, offset, cmdStr,
						this.keyBytes, flag, this.expTime, encodedData.length,
						this.cas);
			}
		} else {
			if (isNoreply()) {
				offset = ByteUtils.setArguments(buf, offset, cmdStr,
						this.keyBytes, flag, this.expTime, encodedData.length,
						this.namespace, this.version, Constants.NO_REPLY);
			} else {
				offset = ByteUtils.setArguments(buf, offset, cmdStr,
						this.keyBytes, flag, this.expTime, encodedData.length,
                        this.namespace, this.version);
			}
		}
		ByteUtils.setArguments(buf, offset, encodedData);
		this.ioBuffer = IoBuffer.wrap(buf);
	}
}
