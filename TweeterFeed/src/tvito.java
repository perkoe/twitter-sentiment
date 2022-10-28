	import java.util.ArrayList;

	import java.util.List;
	import twitter4j.Query;
	import twitter4j.QueryResult;
	import twitter4j.Status;
	import twitter4j.Twitter;
	import twitter4j.TwitterException;
	import twitter4j.TwitterFactory;

	public class tvito {

		public static ArrayList<String> dobiTvite(String tema) {

			Twitter twitter = new TwitterFactory().getInstance();
			ArrayList<String> izpisTvitov = new ArrayList<String>();
			try {
				Query poizvedba = new Query(tema);
				QueryResult result;
				do {
					result = twitter.search(poizvedba);
					List<Status> vsiTviti = result.getTweets();
					for (Status tviti : vsiTviti) {
						izpisTvitov.add(tviti.getText());
					}
				} while ((poizvedba = result.nextQuery()) != null);
			} catch (TwitterException a) {
				a.printStackTrace();
				System.out.println("Failed to search tweets: " + a.getMessage());
			}
			return izpisTvitov;
		}
	}

