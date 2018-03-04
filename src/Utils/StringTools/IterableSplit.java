package Utils.StringTools;

public class IterableSplit
{
	public static String IterableSplittedByComma(Iterable<? extends Object> iters)
	{
		String SplittedString = "---- ";

		for (Object t : iters)
		{
			if (t.equals(""))
			{
				continue;
			}
			SplittedString = SplittedString + t + " , ";
		}
		return SplittedString;
	}
}
