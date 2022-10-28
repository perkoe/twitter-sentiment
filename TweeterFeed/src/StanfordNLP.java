import edu.stanford.nlp.ling.CoreAnnotations;


import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class StanfordNLP {
	static StanfordCoreNLP pipeline;

	public static void nastavi() {
		pipeline = new StanfordCoreNLP("MyPropFile.properties");
	}
	public static int najdiS(String tviti) {

		int glavniS = 0;
		if (tviti != null && tviti.length() > 0) {
			int najdaljsi = 0;
			
			Annotation annotation = pipeline.process(tviti);
			for (CoreMap stavek : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
				Tree tree = stavek.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
				int s = RNNCoreAnnotations.getPredictedClass(tree);
				String delBesedila = stavek.toString();
				if (delBesedila.length() > najdaljsi) {
					glavniS = s;
					najdaljsi = delBesedila.length();
				}

			}
		}
		return glavniS;
	}
}