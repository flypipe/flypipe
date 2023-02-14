import React from 'react';
import { useStore } from './store';


const App = () => {

  const amount = useStore(state => state.counter);
  const incrementAmount = useStore(state => state.incrementAmount);

  return (
    <div>
      <h1>Test: {amount} </h1>
      <button onClick={() => {incrementAmount()}}></button>
    </div>
  )
}
export default App;