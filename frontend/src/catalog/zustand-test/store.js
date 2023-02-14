import { create } from 'zustand';

export const useStore = create((set) => ({
    counter: 5,
    incrementAmount: () => set( ({counter}) => ({counter: counter + 1}) )
}))