package xml;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

@JacksonXmlRootElement(localName = "inventory", namespace = "http://www.demandware.com/xml/impex/inventory/2007-05-31")
public record Inventory (
        @JacksonXmlProperty(localName = "inventory-list")
        InventoryList inventoryList
) {
}