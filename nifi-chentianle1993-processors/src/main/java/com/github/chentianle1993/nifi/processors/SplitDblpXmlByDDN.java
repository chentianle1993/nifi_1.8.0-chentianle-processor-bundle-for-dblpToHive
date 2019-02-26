/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.chentianle1993.nifi.processors;

import com.github.chentianle1993.nifi.processors.split.XmlElementNotifier;
import com.github.chentianle1993.nifi.processors.split.XmlUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.*;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import java.io.InputStream;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nifi.flowfile.attributes.FragmentAttributes.*;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"xml", "split"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Splits an XML File into multiple separate FlowFiles, each comprising a child or descendant of the original root element")
@WritesAttributes({
        @WritesAttribute(attribute = "fragment.identifier",
                description = "All split FlowFiles produced from the same parent FlowFile will have the same randomly generated UUID added for this attribute"),
        @WritesAttribute(attribute = "fragment.index",
                description = "A one-up number that indicates the ordering of the split FlowFiles that were created from a single parent FlowFile"),
        @WritesAttribute(attribute = "fragment.count",
                description = "The number of split FlowFiles generated from the parent FlowFile"),
        @WritesAttribute(attribute = "segment.original.filename ", description = "The filename of the parent FlowFile")
})
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "The entirety of the FlowFile's content (as a Document object) is read into memory, " +
        "in addition to all of the generated FlowFiles representing the split XML. A Document object can take approximately 10 times as much memory as the size of " +
        "the XML. For example, a 1 MB XML document may use 10 MB of memory. If many splits are generated due to the size of the XML, a two-phase approach may be " +
        "necessary to avoid excessive use of memory.")
public class SplitDblpXmlByDDN extends AbstractProcessor {

    private static final PropertyDescriptor SPLIT_DEPTH = new PropertyDescriptor.Builder()
            .name("Split Depth")
            .description("Indicates the XML-nesting depth to start splitting XML fragments. A depth of 1 means split the root's children, whereas a depth of"
                    + " 2 means split the root's children's children and so forth.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .build();

    private static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile that was split into segments. If the FlowFile fails processing, nothing will be sent to this relationship")
            .build();
    public static final Relationship REL_ARTICLE_SPLIT = new Relationship.Builder()
            .name("article split")
            .description("All article segments of the original FlowFile will be routed to this relationship")
            .build();
    public static final Relationship REL_INPROCEEDINGS_SPLIT = new Relationship.Builder()
            .name("inproceedings split")
            .description("All inproceedings segments of the original FlowFile will be routed to this relationship")
            .build();
    public static final Relationship REL_PROCEEDINGS_SPLIT = new Relationship.Builder()
            .name("proceedings split")
            .description("All proceedings segments of the original FlowFile will be routed to this relationship")
            .build();
    public static final Relationship REL_BOOK_SPLIT = new Relationship.Builder()
            .name("book split")
            .description("All book segments of the original FlowFile will be routed to this relationship")
            .build();
    public static final Relationship REL_INCOLLECTION_SPLIT = new Relationship.Builder()
            .name("incollection split")
            .description("All incollection segments of the original FlowFile will be routed to this relationship")
            .build();
    public static final Relationship REL_PHDTHESIS_SPLIT = new Relationship.Builder()
            .name("phdthesis split")
            .description("All phdthesis segments of the original FlowFile will be routed to this relationship")
            .build();
    public static final Relationship REL_MASTERTHESIS_SPLIT = new Relationship.Builder()
            .name("masterthesis split")
            .description("All masterthesis segments of the original FlowFile will be routed to this relationship")
            .build();
    public static final Relationship REL_WWW_SPLIT = new Relationship.Builder()
            .name("www split")
            .description("All www segments of the original FlowFile will be routed to this relationship")
            .build();
//    public static final Relationship REL_PERSON_SPLIT = new Relationship.Builder()
//            .name("person split")
//            .description("All person segments of the original FlowFile will be routed to this relationship")
//            .build();
//    public static final Relationship REL_DATA_SPLIT = new Relationship.Builder()
//            .name("data split")
//            .description("All data segments of the original FlowFile will be routed to this relationship")
//            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile fails processing for any reason (for example, the FlowFile is not valid XML), it will be routed to this relationship")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    private static final String FEATURE_PREFIX = "http://xml.org/sax/features/";
    public static final String ENABLE_NAMESPACES_FEATURE = FEATURE_PREFIX + "namespaces";
    public static final String ENABLE_NAMESPACE_PREFIXES_FEATURE = FEATURE_PREFIX + "namespace-prefixes";
    private static final SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
    public static final String MERGED_RECORDS_NUMBER = "mergedRecordsNumber";

    private static final AtomicInteger xmlTreeType = new AtomicInteger(0);//xml节点类型

    static {
        saxParserFactory.setNamespaceAware(true);
        try {
            saxParserFactory.setFeature(ENABLE_NAMESPACES_FEATURE, true);
            saxParserFactory.setFeature(ENABLE_NAMESPACE_PREFIXES_FEATURE, true);
        } catch (Exception e) {
            final Logger staticLogger = LoggerFactory.getLogger(SplitDblpXmlByDDN.class);
            staticLogger.warn("Unable to configure SAX Parser to make namespaces available", e);
        }
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SPLIT_DEPTH);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_ARTICLE_SPLIT);
        relationships.add(REL_INPROCEEDINGS_SPLIT);
        relationships.add(REL_PROCEEDINGS_SPLIT);
        relationships.add(REL_BOOK_SPLIT);
        relationships.add(REL_INCOLLECTION_SPLIT);
        relationships.add(REL_PHDTHESIS_SPLIT);
        relationships.add(REL_MASTERTHESIS_SPLIT);
        relationships.add(REL_WWW_SPLIT);
//        relationships.add(REL_PERSON_SPLIT);
//        relationships.add(REL_DATA_SPLIT);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final int depth = context.getProperty(SPLIT_DEPTH).asInteger();
        final ComponentLog logger = getLogger();


        final List<FlowFile> articleSplits = new ArrayList<>();//分割结果的保存地址
        final AtomicInteger articleSplitIndex = new AtomicInteger(0);

        final List<FlowFile> inproceedingsSplits = new ArrayList<>();
        final AtomicInteger inproceedingsSplitIndex = new AtomicInteger(0);

        final List<FlowFile> proceedingsSplits = new ArrayList<>();
        final AtomicInteger proceedingsSplitIndex = new AtomicInteger(0);

        final List<FlowFile> bookSplits = new ArrayList<>();
        final AtomicInteger bookSplitIndex = new AtomicInteger(0);

        final List<FlowFile> incollectionSplits = new ArrayList<>();
        final AtomicInteger incollectionSplitIndex = new AtomicInteger(0);

        final List<FlowFile> phdthesisSplits = new ArrayList<>();
        final AtomicInteger phdthesisSplitIndex = new AtomicInteger(0);

        final List<FlowFile> masterthesisSplits = new ArrayList<>();
        final AtomicInteger masterthesisSplitIndex = new AtomicInteger(0);

        final List<FlowFile> wwwSplits = new ArrayList<>();
        final AtomicInteger wwwSplitIndex = new AtomicInteger(0);

//        final List<FlowFile> personSplits = new ArrayList<>();
//        final AtomicInteger personSplitIndex = new AtomicInteger(0);
//
//        final List<FlowFile> dataSplits = new ArrayList<>();
//        final AtomicInteger dataSplitIndex = new AtomicInteger(0);

        final String fragmentIdentifier = UUID.randomUUID().toString();

        final XmlSplitterSaxParser parser = new XmlSplitterSaxParser(xmlTree -> {
            FlowFile split = session.create(original);
            split = session.write(split, out -> out.write(xmlTree.getBytes("UTF-8")));
//            split = session.putAttribute(split, FRAGMENT_ID.key(), fragmentIdentifier);
//            split = session.putAttribute(split, FRAGMENT_ID.key(), fragmentIdentifier);
//            split = session.putAttribute(split, SEGMENT_ORIGINAL_FILENAME.key(), split.getAttribute(CoreAttributes.FILENAME.key()));
            split = session.putAttribute(split, MERGED_RECORDS_NUMBER, "1");
            switch (xmlTreeType.get()){
                case 1:
                    split = session.putAttribute(split, FRAGMENT_INDEX.key(), Integer.toString(articleSplitIndex.getAndIncrement()));
                    articleSplits.add(split);
                    break;
                case 2:
                    split = session.putAttribute(split, FRAGMENT_INDEX.key(), Integer.toString(inproceedingsSplitIndex.getAndIncrement()));
                    inproceedingsSplits.add(split);
                    break;
                case 3:
                    split = session.putAttribute(split, FRAGMENT_INDEX.key(), Integer.toString(proceedingsSplitIndex.getAndIncrement()));
                    proceedingsSplits.add(split);
                    break;
                case 4:
                    split = session.putAttribute(split, FRAGMENT_INDEX.key(), Integer.toString(bookSplitIndex.getAndIncrement()));
                    bookSplits.add(split);
                    break;
                case 5:
                    split = session.putAttribute(split, FRAGMENT_INDEX.key(), Integer.toString(incollectionSplitIndex.getAndIncrement()));
                    incollectionSplits.add(split);
                    break;
                case 6:
                    split = session.putAttribute(split, FRAGMENT_INDEX.key(), Integer.toString(phdthesisSplitIndex.getAndIncrement()));
                    phdthesisSplits.add(split);
                    break;
                case 7:
                    split = session.putAttribute(split, FRAGMENT_INDEX.key(), Integer.toString(masterthesisSplitIndex.getAndIncrement()));
                    masterthesisSplits.add(split);
                    break;
                case 8:
                    split = session.putAttribute(split, FRAGMENT_INDEX.key(), Integer.toString(wwwSplitIndex.getAndIncrement()));
                    wwwSplits.add(split);
                    break;
//                case 9:
//                    split = session.putAttribute(split, FRAGMENT_INDEX.key(), Integer.toString(personSplitIndex.getAndIncrement()));
//                    personSplits.add(split);
//                    break;
//                case 10:
//                    split = session.putAttribute(split, FRAGMENT_INDEX.key(), Integer.toString(dataSplitIndex.getAndIncrement()));
//                    dataSplits.add(split);
//                    break;
                default:break;
            }
        }, depth);//注册分割xml的程序

        final AtomicBoolean failed = new AtomicBoolean(false);
        session.read(original, rawIn -> {
            try (final InputStream in = new java.io.BufferedInputStream(rawIn)) {
                try {
                    final XMLReader reader = XmlUtils.createSafeSaxReader(saxParserFactory, parser);
                    reader.parse(new InputSource(in));
                } catch (final ParserConfigurationException | SAXException e) {
                    logger.error("Unable to parse {} due to {}", new Object[]{original, e});
                    failed.set(true);
                }
            }
        });

        if (failed.get()) {
            session.transfer(original, REL_FAILURE);
            session.remove(articleSplits);
            session.remove(inproceedingsSplits);
            session.remove(proceedingsSplits);
            session.remove(bookSplits);
            session.remove(incollectionSplits);
            session.remove(phdthesisSplits);
            session.remove(masterthesisSplits);
            session.remove(wwwSplits);
//            session.remove(personSplits);
//            session.remove(dataSplits);
        } else {
            articleSplits.forEach((split) -> {
                String index = split.getAttribute(FRAGMENT_INDEX.key());
                String count = String.valueOf((int) articleSplitIndex.get());
                String ddn = String.format("0/8.%s/%s",index,count);// 1/10.1/1000
                split = session.putAttribute(split, DOTTED_DECIMAL_NOTATION.key(), ddn);//分割
                session.transfer(split, REL_ARTICLE_SPLIT);
            });
            inproceedingsSplits.forEach((split) -> {
                String index = split.getAttribute(FRAGMENT_INDEX.key());
                String count = String.valueOf((int) inproceedingsSplitIndex.get());
                String ddn = String.format("1/8.%s/%s",index,count);// 1/10.1/1000
                split = session.putAttribute(split, DOTTED_DECIMAL_NOTATION.key(), ddn);//分割
                session.transfer(split, REL_INPROCEEDINGS_SPLIT);
            });
            proceedingsSplits.forEach((split) -> {
                String index = split.getAttribute(FRAGMENT_INDEX.key());
                String count = String.valueOf((int) proceedingsSplitIndex.get());
                String ddn = String.format("2/8.%s/%s",index,count);// 1/10.1/1000
                split = session.putAttribute(split, DOTTED_DECIMAL_NOTATION.key(), ddn);//分割
                session.transfer(split, REL_PROCEEDINGS_SPLIT);
            });
            bookSplits.forEach((split) -> {
                String index = split.getAttribute(FRAGMENT_INDEX.key());
                String count = String.valueOf((int) bookSplitIndex.get());
                String ddn = String.format("3/8.%s/%s",index,count);// 1/10.1/1000
                split = session.putAttribute(split, DOTTED_DECIMAL_NOTATION.key(), ddn);//分割
                session.transfer(split, REL_BOOK_SPLIT);
            });
            incollectionSplits.forEach((split) -> {
                String index = split.getAttribute(FRAGMENT_INDEX.key());
                String count = String.valueOf((int) incollectionSplitIndex.get());
                String ddn = String.format("4/8.%s/%s",index,count);// 1/10.1/1000
                split = session.putAttribute(split, DOTTED_DECIMAL_NOTATION.key(), ddn);//分割
                session.transfer(split, REL_INCOLLECTION_SPLIT);
            });
            phdthesisSplits.forEach((split) -> {
                String index = split.getAttribute(FRAGMENT_INDEX.key());
                String count = String.valueOf((int) phdthesisSplitIndex.get());
                String ddn = String.format("5/8.%s/%s",index,count);// 1/10.1/1000
                split = session.putAttribute(split, DOTTED_DECIMAL_NOTATION.key(), ddn);//分割
                session.transfer(split, REL_PHDTHESIS_SPLIT);
            });
            masterthesisSplits.forEach((split) -> {
                String index = split.getAttribute(FRAGMENT_INDEX.key());
                String count = String.valueOf((int) masterthesisSplitIndex.get());
                String ddn = String.format("6/8.%s/%s",index,count);// 1/10.1/1000
                split = session.putAttribute(split, DOTTED_DECIMAL_NOTATION.key(), ddn);//分割
                session.transfer(split, REL_MASTERTHESIS_SPLIT);
            });
            wwwSplits.forEach((split) -> {
                String index = split.getAttribute(FRAGMENT_INDEX.key());
                String count = String.valueOf((int) wwwSplitIndex.get());
                String ddn = String.format("7/8.%s/%s",index,count);// 1/10.1/1000
                split = session.putAttribute(split, DOTTED_DECIMAL_NOTATION.key(), ddn);//分割
                session.transfer(split, REL_WWW_SPLIT);
            });
//            personSplits.forEach((split) -> {
//                String index = split.getAttribute(FRAGMENT_INDEX.key());
//                String count = String.valueOf((int) personSplitIndex.get());
//                String ddn = String.format("8/10.%s/%s",index,count);// 1/10.1/1000
//                split = session.putAttribute(split, DOTTED_DECIMAL_NOTATION.key(), ddn);//分割
//                session.transfer(split, REL_PERSON_SPLIT);
//            });
//            dataSplits.forEach((split) -> {
//                String index = split.getAttribute(FRAGMENT_INDEX.key());
//                String count = String.valueOf((int) dataSplitIndex.get());
//                String ddn = String.format("9/10.%s/%s",index,count);// 1/10.1/1000
//                split = session.putAttribute(split, DOTTED_DECIMAL_NOTATION.key(), ddn);//分割
//                session.transfer(split, REL_DATA_SPLIT);
//            });

            int allSplitsCount = articleSplitIndex.get()
                                +inproceedingsSplitIndex.get()
                                +proceedingsSplitIndex.get()
                                +bookSplitIndex.get()
                                +incollectionSplitIndex.get()
                                +phdthesisSplitIndex.get()
                                +masterthesisSplitIndex.get()
                                +wwwSplitIndex.get();
//                                +personSplitIndex.get()
//                                +dataSplitIndex.get();

            final FlowFile originalToTransfer = copyAttributesToOriginal(session, original, fragmentIdentifier,allSplitsCount);
            session.transfer(originalToTransfer, REL_ORIGINAL);
            logger.info("Split {} into {} FlowFiles", new Object[]{originalToTransfer, allSplitsCount});
        }
    }

    private static class XmlSplitterSaxParser implements ContentHandler {

        private static final String XML_PROLOGUE = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
        private final XmlElementNotifier notifier;
        private final int splitDepth;
        private final StringBuilder sb = new StringBuilder(XML_PROLOGUE);
        private int depth = 0;
        private Map<String, String> prefixMap = new TreeMap<>();

        public XmlSplitterSaxParser(XmlElementNotifier notifier, int splitDepth) {
            this.notifier = notifier;
            this.splitDepth = splitDepth;
        }

        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
            // if we're not at a level where we care about capturing text, then return
            if (depth <= splitDepth) {
                return;
            }

            // capture text
            for (int i = start; i < start + length; i++) {
                char c = ch[i];
                switch (c) {
                    case '<':
                        sb.append("&lt;");
                        break;
                    case '>':
                        sb.append("&gt;");
                        break;
                    case '&':
                        sb.append("&amp;");
                        break;
                    case '\'':
                        sb.append("&apos;");
                        break;
                    case '"':
                        sb.append("&quot;");
                        break;
                    default:
                        sb.append(c);
                        break;
                }
            }
        }

        @Override
        public void endDocument() throws SAXException {
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            // We have finished processing this element. Decrement the depth.
            int newDepth = --depth;//当前元素深度

            // if we're at a level where we care about capturing text, then add the closing element
            if (newDepth >= splitDepth) {
                // Add the element end tag.
                sb.append("</").append(qName).append(">");
            }

            // If we have now returned to level 1, we have finished processing
            // a 2nd-level element. Send notification with the XML text and
            // erase the String Builder so that we can start
            // processing a new 2nd-level element.
            if (newDepth == splitDepth) {
                switch(qName){
                    case "article":
                        xmlTreeType.set(1);
                        break;
                    case "inproceedings":
                        xmlTreeType.set(2);
                        break;
                    case "proceedings":
                        xmlTreeType.set(3);
                        break;
                    case "book":
                        xmlTreeType.set(4);
                        break;
                    case "incollection":
                        xmlTreeType.set(5);
                        break;
                    case "phdthesis":
                        xmlTreeType.set(6);
                        break;
                    case "mastersthesis":
                        xmlTreeType.set(7);
                        break;
                    case "www":
                        xmlTreeType.set(8);
                        break;
//                    case "person":
//                        xmlTreeType.set(9);
//                        break;
//                    case "data":
//                        xmlTreeType.set(10);
//                        break;
                    default:
                        throw new ProcessException("the xml has attribute other than article, inproceedings, proceedings, book, and so on");
                }
                String elementTree = sb.toString();
                notifier.onXmlElementFound(elementTree);
                // Reset the StringBuilder to just the XML prolog.
                sb.setLength(XML_PROLOGUE.length());
            }
        }

        @Override
        public void endPrefixMapping(String prefix) throws SAXException {
            prefixMap.remove(prefixToNamespace(prefix));
        }

        @Override
        public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        }

        @Override
        public void processingInstruction(String target, String data) throws SAXException {
        }

        @Override
        public void setDocumentLocator(Locator locator) {
        }

        @Override
        public void skippedEntity(String name) throws SAXException {
        }

        @Override
        public void startDocument() throws SAXException {
        }

        @Override
        public void startElement(final String uri, final String localName, final String qName, final Attributes atts) throws SAXException {
            // Increment the current depth because start a new XML element.
            int newDepth = ++depth;//当前元素深度
            // Output the element and its attributes if it is
            // not the root element.
            if (newDepth > splitDepth) {
                sb.append("<");
                sb.append(qName);

                final Set<String> attributeNames = new HashSet<>();
                int attCount = atts.getLength();
                for (int i = 0; i < attCount; i++) {
                    String attName = atts.getQName(i);
                    attributeNames.add(attName);
                    String attValue = StringEscapeUtils.escapeXml10(atts.getValue(i));
                    sb.append(" ").append(attName).append("=").append("\"").append(attValue).append("\"");
                }

                // If this is the first node we're outputting write out
                // any additional namespace declarations that are required
                if (splitDepth == newDepth - 1) {
                    for (Entry<String, String> entry : prefixMap.entrySet()) {
                        // If we've already added this namespace as an attribute then continue
                        if (attributeNames.contains(entry.getKey())) {
                            continue;
                        }
                        sb.append(" ");
                        sb.append(entry.getKey());
                        sb.append("=\"");
                        sb.append(entry.getValue());
                        sb.append("\"");
                    }
                }

                sb.append(">");
            }
        }

        @Override
        public void startPrefixMapping(String prefix, String uri) throws SAXException {
            final String ns = prefixToNamespace(prefix);
            prefixMap.put(ns, uri);
        }

        private String prefixToNamespace(String prefix) {
            final String ns;
            if (prefix.length() == 0) {
                ns = "xmlns";
            } else {
                ns="xmlns:"+prefix;
            }
            return ns;
        }
    }

}
