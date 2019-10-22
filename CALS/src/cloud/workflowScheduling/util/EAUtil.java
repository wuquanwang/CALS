package cloud.workflowScheduling.util;

import java.util.*;

/**
 * Simulated Binary Crossover (SBX), Polynomial Mutation (PM)
 * @author wu
 */
public class EAUtil {
	private static final double EPS = 1.0e-14;
	private static double distributionIndex_ = 5.0;
	private static double eta_m_ = 20.0;
	private static Random random = new Random();

	public static double[] SBX(double valueX1, double valueX2, double yL, double yu) {
		double y1, y2;
		double[] c = new double[2];
		double alpha, beta, betaq;
		if (java.lang.Math.abs(valueX1 - valueX2) < EPS) {
			c[0] = valueX1;
			c[1] = valueX2;
			return c;
		}

		if (valueX1 < valueX2) {
			y1 = valueX1;
			y2 = valueX2;
		} else {
			y1 = valueX2;
			y2 = valueX1;
		} // if

		double rand = random.nextDouble();
		beta = 1.0 + (2.0 * (y1 - yL) / (y2 - y1));
		alpha = 2.0 - java.lang.Math.pow(beta, -(distributionIndex_ + 1.0));

		if (rand <= (1.0 / alpha)) {
			betaq = java.lang.Math.pow((rand * alpha), (1.0 / (distributionIndex_ + 1.0)));
		} else {
			betaq = java.lang.Math.pow((1.0 / (2.0 - rand * alpha)), (1.0 / (distributionIndex_ + 1.0)));
		} // if

		c[0] = 0.5 * ((y1 + y2) - betaq * (y2 - y1));

		beta = 1.0 + (2.0 * (yu - y2) / (y2 - y1));
		alpha = 2.0 - java.lang.Math.pow(beta, -(distributionIndex_ + 1.0));

		if (rand <= (1.0 / alpha)) {
			betaq = java.lang.Math.pow((rand * alpha), (1.0 / (distributionIndex_ + 1.0)));
		} else {
			betaq = java.lang.Math.pow((1.0 / (2.0 - rand * alpha)), (1.0 / (distributionIndex_ + 1.0)));
		} // if

		c[1] = 0.5 * ((y1 + y2) + betaq * (y2 - y1));

		if (c[0] < yL)
			c[0] = yL;
		if (c[1] < yL)
			c[1] = yL;
		if (c[0] > yu)
			c[0] = yu;
		if (c[1] > yu)
			c[1] = yu;

//		System.out.println(c[0] + "\t" + c[1]);

		return c;
	} // if

	public static double PM(double y, double yl, double yu) {
		double delta1, delta2, mut_pow, deltaq;
		double val, xy;
		delta1 = (y - yl) / (yu - yl);
		delta2 = (yu - y) / (yu - yl);
		double rnd = random.nextDouble();
		mut_pow = 1.0 / (eta_m_ + 1.0);
		if (rnd <= 0.5) {
			xy = 1.0 - delta1;
			val = 2.0 * rnd + (1.0 - 2.0 * rnd) * (Math.pow(xy, (eta_m_ + 1.0)));
			deltaq = java.lang.Math.pow(val, mut_pow) - 1.0;
		} else {
			xy = 1.0 - delta2;
			val = 2.0 * (1.0 - rnd) + 2.0 * (rnd - 0.5) * (java.lang.Math.pow(xy, (eta_m_ + 1.0)));
			deltaq = 1.0 - (java.lang.Math.pow(val, mut_pow));
		}
		y = y + deltaq * (yu - yl);
		if (y < yl)
			y = yl;
		if (y > yu)
			y = yu;
		return y;
	}

	//local test
	public static void main(String[] args) {
		for (int i = 0; i < 10; i++) {
			double[] arr = SBX(0.2, 0.7, 0, 1);
			System.out.println(arr[0] + "\t" + arr[1]);
//			System.out.println(PM(0.3, 0, 1));
		}
	}
}
