package samzaapps.Nexmark.serde;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * An ad click event.
 */
public class Bid {

    private long auctionId; // the unique id of the page that the ad was clicked on
    private long bidder; // an unique id for the ad
    private long price; // the user that clicked the ad
    private long dateTime; // the user that clicked the ad
    private String extra; // the user that clicked the ad

    public Bid(
            @JsonProperty("auctionId") long auctionId,
            @JsonProperty("bidder") long bidder,
            @JsonProperty("price") long price,
            @JsonProperty("dataTime") long dateTime,
            @JsonProperty("extra") String extra){
        this.auctionId = auctionId;
        this.bidder = bidder;
        this.price = price;
        this.dateTime = dateTime;
        this.extra = extra;
    }

    public long getAuctionId() {
        return auctionId;
    }

    public void setAuctionId(long auctionId) {
        this.auctionId = auctionId;
    }

    public long getBidder() {
        return bidder;
    }

    public void setPrice(long price) {
        this.price = price;
    }

    public long getPrice() {
        return price;
    }

    public long getDateTime() {
        return dateTime;
    }

    public String getExtra() {
        return extra;
    }

}