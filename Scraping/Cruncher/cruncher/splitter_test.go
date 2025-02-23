package main

import (
	"log"
	"testing"
)

func TestMapping(t *testing.T) {
	mapping := BuildSearchTermMapping("../../Tally/search_terms_reduced.csv")

	text := `onurdongel
Investment thesis
There is nothing better than having a number of stocks in your portfolio that increase their dividend like clockwork. There are plenty of people who don\'t invest in boring stocks, while I think we should embrace boring sometimes. Companies that are dominant in their sector and have a proven business model. These companies keep growing earnings and can almost guarantee dividend increases despite the influence of the economic cycle.
One of my own top 5 holdings is A. O. Smith Corporation (NYSE:AOS). The company doesn\'t get the attention it deserves. In my view this is unjustified, because the long-term results are exciting.
AOS total return (YCharts)
For the dividend growth investors who have a lot of time on their side, there is a lot to like as well. The dividend yield is "just" 1.49% at the moment but that\'s just because of its share price appreciation.
AOS dividend summary (Seeking Alpha)
With a 5Y dividend growth CAGR of 9% and a healthy pay-out ratio of 31.9% there is no sign of weakness. I even think the company has serious dividend king potential.
Today I want to update my investment thesis for the company using the latest information.
Why AOS?

(RTTNews) - 3M Company (MMM), Tuesday announced the launch of 3M WorkTunes Connect + Solar Hearing Protector, a first-of-its-kind solar charging wireless Bluetooth hearing protector for consumers.
With the solar cell technology Powerfoyle, the headset continuously charges itself using any available light source, whether indoors or outdoors.
Also, it has a Noise Reduction Rating of 26 Db, helping to reduce the risk of hearing loss in customers.
The headset is now available for purchase at a price of $169.99.

Why invest in AOS? I will give you a summary of why you should take a look at the company. For people who are interested in additional information about AOS, you can read my previous article via this link. 
The company has a stable business model and they are very profitable in the US. `

	log.Println(TagText(text, "", mapping))
}
