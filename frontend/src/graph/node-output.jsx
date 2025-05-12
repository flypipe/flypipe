import React from "react";
import { Badge, Accordion } from "react-bootstrap";
import DOMPurify from "dompurify";

import { formatTextToHtml } from "../util";

export const NodeOutput = ({ output }) => {
    const outputs = [];

    output.forEach((out) => {
        outputs.push(
            <Accordion.Item eventKey={out.column} key={out.column}>
                <Accordion.Header>
                    <div className="d-flex">
                        <div className="mr-auto p-2">{out.column}</div>
                        <div className="p-2">
                            <Badge bg="secondary">{out.type}</Badge>
                        </div>
                    </div>
                </Accordion.Header>
                <Accordion.Body
                    dangerouslySetInnerHTML={{
                        __html: DOMPurify.sanitize(
                            formatTextToHtml(out.description)
                        ),
                    }}
                />
            </Accordion.Item>
        );
    });

    return <Accordion>{outputs}</Accordion>;
};
