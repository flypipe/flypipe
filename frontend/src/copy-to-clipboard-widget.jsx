import React from "react";
import { CopyToClipboard } from "react-copy-to-clipboard";
import { BsClipboard } from "react-icons/bs";
import Tooltip from "./tooltip";
import { Button } from "react-bootstrap";

const CopyToClipboardWidget = ({ text, data, onCopy, className }) => {
    return (
        <>
            {text && <span className="ms-2 text-secondary">{text}</span>}
            <CopyToClipboard
                text={data}
                className="mx-2 pointer fst-italic text-secondary bg-opacity-10"
                onCopy={onCopy}
            >
                <span>
                    <Tooltip text={"Copy to Clipboard"}>
                        <Button
                            variant="light"
                            className={`${className} btn-sm position-absolute`}
                        >
                            <BsClipboard />
                        </Button>
                    </Tooltip>
                </span>
            </CopyToClipboard>
        </>
    );
};

export default CopyToClipboardWidget;
