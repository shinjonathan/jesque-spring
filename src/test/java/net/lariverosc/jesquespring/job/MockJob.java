package net.lariverosc.jesquespring.job;

/**
 *
 * @author Alejandro <lariverosc@gmail.com>
 */
public class MockJob implements Runnable {

	public static int JOB_COUNT = 0;

	@Override
	public void run() {
		JOB_COUNT++;
	}
}
