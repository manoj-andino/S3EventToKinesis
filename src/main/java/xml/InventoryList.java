package xml;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import java.util.List;


public record InventoryList (
        @JacksonXmlProperty(localName = "header")
        Header header,
        @JacksonXmlElementWrapper(localName = "records")
        List<Record> records
) {
}