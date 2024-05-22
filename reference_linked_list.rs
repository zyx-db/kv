use std::mem;
use std::rc::Rc;
use std::sync::{RwLock, RwLockWriteGuard};

struct Node {
    data: u8,
    next: Option<Rc<RwLock<Node>>>,
}

impl Node {
    fn new(data: u8) -> Self {
        Node { data, next: None }
    }

    fn add_one_print(&mut self) {
        self.data += 1;
        print!("({}) -> ", self.data);
        match &self.next {
            Some(rwlock) => {
                let mut next = rwlock.write().unwrap();
                next.add_one_print();
            }
            None => {}
        }
    }
}

struct LinkedList {
    dummy_node_head: Node,
}

impl LinkedList {
    fn init(vals: Vec<u8>) -> Self {
        let mut dummy_node_head = Node::new(0);
        for v in vals.iter().rev() {
            let mut new_node = Node::new(*v);
            match &dummy_node_head.next {
                Some(h) => {
                    new_node.next = Some(Rc::clone(h));
                }
                None => {}
            }
            dummy_node_head.next = Some(Rc::new(RwLock::new(new_node)));
        }

        LinkedList { dummy_node_head }
    }

    fn increment_print_rec(&self) {
        match &self.dummy_node_head.next {
            Some(x) => {
                x.write().unwrap().add_one_print();
            }
            None => {}
        }
    }

    fn increment_print(&self) {
        if let Some(incoming) = &self.dummy_node_head.next {
            let mut node = Rc::clone(incoming);
            let mut lock = incoming.write().unwrap();
            lock.data += 1;
            print!("({}) -> ", lock.data);

            while lock.next.is_some(){
                node = Rc::clone(lock.next.as_ref().unwrap());
                lock = unsafe {mem::transmute(node.write().unwrap())};
                lock.data += 1;
                print!("({}) -> ", lock.data);
            }
            println!();
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_traverse() {
        let list = LinkedList::init(vec![1, 2, 3]);
        list.increment_print();
        list.increment_print();
        list.increment_print();
    }
}
