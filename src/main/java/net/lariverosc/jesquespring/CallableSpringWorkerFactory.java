/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.lariverosc.jesquespring;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.worker.WorkerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.Collection;
import java.util.concurrent.Callable;

/**
 *
 * @author Alejandro <lariverosc@gmail.com>
 */
public class CallableSpringWorkerFactory implements Callable<WorkerImpl>, ApplicationContextAware {

	private Logger logger = LoggerFactory.getLogger(CallableSpringWorkerFactory.class);
	private final Config config;
	private final Collection<String> queues;
	private ApplicationContext applicationContext;

	/**
	 * Creates a new factory for <code>SpringWorker</code> that use the provided arguments.
	 *
	 * @param config used to create a connection to Redis
	 * @param queues the list of queues to poll
	 */
	public CallableSpringWorkerFactory(final Config config, final Collection<String> queues) {
		this.config = config;
		this.queues = queues;
	}


	/**
	 * Create a new <code>SpringWorker</code> using the arguments provided in the factory constructor.
	 */
	@Override
	public WorkerImpl call() {
		logger.info("Create new Spring Worker");
		WorkerImpl springWorker = new CallableSpringWorker(this.config, this.queues);
		((CallableSpringWorker) springWorker).setApplicationContext(this.applicationContext);
		return springWorker;
	}

	
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}
}
