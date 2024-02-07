package xml;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public record Record (
        @JacksonXmlProperty(localName = "product-id", isAttribute = true)
        Integer productId,
        @JacksonXmlProperty(localName = "preorder-backorder-handling")
        String preorderBackorderHandling,
        @JacksonXmlProperty(localName = "allocation")
        Integer allocation,
        @JacksonXmlProperty(localName = "allocation-timestamp")
        String allocationTimestamp,
        @JacksonXmlProperty(localName = "perpetual")
        Boolean perpetual
) {

}
