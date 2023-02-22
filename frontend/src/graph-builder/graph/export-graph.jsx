import React, {useEffect, useCallback, useContext} from 'react';
import { BsDownload } from "react-icons/bs";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import { CopyToClipboard } from "react-copy-to-clipboard";
import { Button } from "react-bootstrap";
import { useReactFlow } from 'reactflow';
import Tooltip from '../../tooltip';
import { NotificationContext } from "../../context";
import uuid from "react-uuid";
import { generateCodeTemplate } from '../util';


// The CopyToClipboardWidget and it's dependent CopyToClipboard require that the string that is put into the 
// clipboard is supplied as a prop, the string we want to copy (being all the generated source code from new 
// nodes) needs to be re-generated whenever the node data of any node changes or when a new node is added. 
// Unfortunately there is no easy way to recalculate when this happens- we can use useNodes from React Flow 
// but this will recalculate on every single node positional change too which will be very expensive to keep 
// recomputing. 
// The solution is to use our own copy to clipboard function instead of the widget so we can calculate the 
// string to copy on the fly. 
const ExportGraph = () => {
    const graph = useReactFlow();
    const { setNewMessage } = useContext(NotificationContext);

    const onCopyToClipboard = useCallback(() => {
        const newNodes = graph.getNodes().filter(node => node.data.isNew);
        const newNodeNames = newNodes.map(node => node.data.name);

        const newNodesSourceCode = newNodes.map(node => generateCodeTemplate(node.data));
        
        setNewMessage({
            msgId: uuid(),
            message: `Copied code definitions for new nodes ${newNodeNames.join(', ')}`
        });

        navigator.clipboard.writeText(newNodesSourceCode.join('\n\n'));
    }, [graph]);

    return <Tooltip text="Copy the code definitions for any new nodes" placement="left">
        <Button
            variant="outline-secondary flypipe"
            data-toggle="tooltip"
            size="md"
            onClick={onCopyToClipboard}
        >
                <BsDownload />
        </Button>  
    </Tooltip>;
};


export default ExportGraph;