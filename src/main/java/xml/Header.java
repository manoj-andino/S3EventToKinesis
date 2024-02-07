package xml;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public record Header (
        @JacksonXmlProperty(localName = "list-id", isAttribute = true)
        String listId,
        @JacksonXmlProperty(localName = "default-instock")
        Boolean defaultInstock,
        @JacksonXmlProperty(localName = "use-bundle-inventory-only")
        Boolean useBundleInventoryOnly,
        @JacksonXmlProperty(localName = "on-order")
        Boolean onOrder){

}