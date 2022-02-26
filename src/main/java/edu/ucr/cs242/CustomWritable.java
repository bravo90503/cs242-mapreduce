package edu.ucr.cs242;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.io.Writable;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class CustomWritable implements Writable {
	private String docId;
	private long frequency;
	private long position;
	private String positions;

	public CustomWritable() {

	}

	public CustomWritable(long frequency, String positions) {
		this.docId = "";
		this.frequency = frequency;
		this.positions = positions;
	}

	public CustomWritable(String docId, long frequency, long position) {
		this.docId = docId;
		this.frequency = frequency;
		this.position = position;
		this.positions = "";
	}

	public String getDocId() {
		return docId;
	}

	public void setDocId(String docId) {
		this.docId = docId;
	}

	public String getPositions() {
		return positions;
	}

	public void setPositions(String positions) {
		this.positions = positions;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(docId);
		out.writeLong(frequency);
		out.writeLong(position);
		out.writeBytes(positions);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		docId = in.readUTF();
		frequency = in.readLong();
		position = in.readLong();
		positions = in.readLine();
	}

	public long getFrequency() {
		return frequency;
	}

	public void setFrequency(long frequency) {
		this.frequency = frequency;
	}

	public long getPosition() {
		return position;
	}

	public void setPosition(long position) {
		this.position = position;
	}

	@Override
	public String toString() {
		return getJsonString();
	}

	public String getJsonString() {
		JSONObject root = new JSONObject();
		try {
			root.put("frequency", frequency);
			if (positions != null) {
				addDocuments(root);
			}
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (Exception ex) {
			System.out.println(positions.toString());
			ex.printStackTrace();
		} finally {

		}

		return root.toString();
	}

	public void addDocuments(JSONObject root) throws JSONException {
		JSONArray documents = new JSONArray();
		root.put("documents", documents);

		String[] docs = positions.split(";");
		double totalDocs = docs.length;
		// calculate tf*idf ordered by highest score first or descending order
		PriorityQueue<Score> scores = new PriorityQueue<>(docs.length, new ScoreComparator());
		for (String doc : docs) {
			String[] docInfo = doc.split(":");
			String docId = docInfo[0];
			String positionTokens = docInfo[1];
			JSONObject docJsonObject = new JSONObject();
			docJsonObject.put("docId", docId);

			int numberOfTerms = addPositions(positionTokens, new JSONArray(), docJsonObject);
			double score = numberOfTerms * Math.log10((totalDocs / frequency + 1));
			DecimalFormat df = new DecimalFormat("#.####");
			docJsonObject.put("score", df.format(score));
			scores.add(new Score(docJsonObject, score));
		}

		// add sorted documents to root object
		for (Score score : scores) {
			JSONObject doc = score.getDocument();
			documents.put(doc);
		}
	}

	class ScoreComparator implements Comparator<Score> {
		// Overriding compare()method of Comparator
		// for descending order of cgpa
		public int compare(Score s1, Score s2) {
			if (s1.score < s2.score)
				return 1;
			else if (s1.score > s2.score)
				return -1;
			return 0;
		}
	}

	class Score {
		public JSONObject document;
		public Double score;

		public Score(JSONObject document, Double score) {
			this.document = document;
			this.score = score;
		}

		public Double getScore() {
			return score;
		}

		public JSONObject getDocument() {
			return document;
		}
	}

	public int addPositions(String positionTokens, JSONArray positions, JSONObject docJsonObject) throws JSONException {
		String[] splits = positionTokens.split(",");
		List<Integer> numbers = new ArrayList<Integer>();
		for (String split : splits) {
			if (split.length() > 0) {
				numbers.add(Integer.valueOf(split));
			}
		}
		Collections.sort(numbers);
		for (int n : numbers) {
			positions.put(n);
		}
		docJsonObject.put("positions", positions);

		return numbers.size();
	}

}
