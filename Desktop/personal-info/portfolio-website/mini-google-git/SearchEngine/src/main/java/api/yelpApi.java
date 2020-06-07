package api;


import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class yelpApi {
	private static final String YELP_HOST = "https://api.yelp.com";
	private static final String YELP_API_VERSION = "/v3";
	private static final String BUSINESSES_SEARCH_ENDPOINT = "/businesses/search";
	private static final String BUSINESSES_MATCHES_ENDPOINT = "/businesses/matches";
	private static final String CLIENT_ID = "BQAHahlWGMabHTQ8wjbn_Q";
	private static final String API_KEY = "3WAS1hCFa2a_-LZtumXMQlqtLd9gg8cgCwrWXf7TcdWU5GYIJTaCsNbPowr56li5NhVuhnz8k7zpW6EGn9S3k3HvLovFbtwkmVjVhtuqn2tlHx74IP-3Zft9NoexXnYx";
	private static final String BUSINESS_PATH = "/v3/businesses";
	
	private static final String AUTHORIZATION = "Authorization";
	private static final String BEARER = "Bearer ";
	
		//GET https://api.yelp.com/v3/autocomplete?text=del&latitude=37.786882&longitude=-122.399972
	public static JSONObject connectToApi(String term, String location) {
		String url = YELP_HOST + YELP_API_VERSION + BUSINESSES_SEARCH_ENDPOINT +  "?term=" + term + "&location=" + location;
        final String USER_AGENT = "cis455";
        String responseText;
        JSONObject myObject = null;
        try {
            Document doc = Jsoup.connect(url)
                    .userAgent(USER_AGENT)
                    .header(AUTHORIZATION, BEARER + API_KEY)
                    .ignoreContentType(true)
                    .get();

            responseText = doc.text();
            myObject = new JSONObject(responseText);
            return myObject;
        } catch(Exception e){
//        	"https://api.yelp.com/v3/businesses/search?latitude=37.786882&longitude=-122.399972"
        }
		return myObject;
    }
	
	public static yelpInfo searchCurrent(JSONObject obj) {
		yelpInfo yelpInfo = new yelpInfo();
		JSONObject page = obj.getJSONObject("region");
		if(page == null) return yelpInfo;
		JSONObject pageName = page.getJSONObject("center");
		if(pageName == null) return yelpInfo;
		JSONArray businesses = obj.getJSONArray("businesses");
		if(businesses.isEmpty()) {
			return yelpInfo;
		}

		
		JSONObject business = obj.getJSONArray("businesses").getJSONObject(0);
		System.out.println(business);
		yelpInfo.distance = business.get("distance").toString();
		yelpInfo.img = business.get("image_url").toString();
		yelpInfo.rating = business.get("rating").toString();
		yelpInfo.phone = business.get("phone").toString();
		yelpInfo.location = business.get("location").toString();
		return yelpInfo;
		
	}

		//term=by-${foodType}&location=${foodLocation}
	public static void main(String[] args) {
//		String url = YELP_HOST + YELP_API_VERSION + BUSINESSES_SEARCH_ENDPOINT +  "?term=Starbucks&location=PA";
		JSONObject obj = connectToApi("pizza","Canton");
		System.out.println(obj);
		System.out.println("-----------------------");
		JSONObject obj1 = connectToApi("lucky","jersy");
		System.out.println(obj1);
		
		yelpInfo yelpinfo = searchCurrent(obj);
		System.out.println(yelpinfo.location);
	}
	

}
