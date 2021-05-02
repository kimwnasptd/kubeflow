import { Injectable } from '@angular/core';
import { K8sObject } from '../utils/kubernetes.model';
import { fromEvent, Subscription, BehaviorSubject } from 'rxjs';
import { map } from 'rxjs/operators';
import { SSE_WATCH_EVENT, WATCH_EVENT_TYPE } from '../enums/watch';
import { WatchEvent } from '../models/watch';

@Injectable({
  providedIn: 'root',
})
export class WatchService {
  private eventSources: { [url: string]: EventSource } = {};
  private eventSubs: { [url: string]: Subscription } = {};
  private data: { [url: string]: K8sObject[] } = {};
  private subjects: { [url: string]: BehaviorSubject<K8sObject[]> } = {};

  constructor() {}

  private addObject(objects: K8sObject[], newObj: K8sObject): K8sObject[] {
    objects.push(newObj);

    // sort based on the name
    objects.sort((a, b) => {
      if (a.metadata.name < b.metadata.name) {
        return -1;
      }

      if (a.metadata.name > b.metadata.name) {
        return 1;
      }

      return 0;
    });

    return objects;
  }

  private updateObject(objects: K8sObject[], newObj: K8sObject): K8sObject[] {
    let index = -1;
    for (const obj of objects) {
      if (obj.metadata.uid !== newObj.metadata.uid) {
        continue;
      }

      index = objects.indexOf(obj);
    }

    objects[index] = newObj;
    return objects;
  }

  private deleteObject(objects: K8sObject[], newObj: K8sObject): K8sObject[] {
    let index = -1;
    for (const obj of objects) {
      if (obj.metadata.uid !== newObj.metadata.uid) {
        continue;
      }

      index = objects.indexOf(obj);
    }

    objects.splice(index, 1);
    return objects;
  }

  private handleListEvent(url): Subscription {
    return fromEvent(this.eventSources[url], SSE_WATCH_EVENT.LIST).subscribe(
      (event: MessageEvent) => {
        this.data[url] = JSON.parse(event.data) as K8sObject[];
        this.subjects[url].next(this.data[url]);
      },
    );
  }

  private handlePageEvent(url): Subscription {
    return fromEvent(this.eventSources[url], SSE_WATCH_EVENT.PAGE).subscribe(
      (event: MessageEvent) => {
        const page = JSON.parse(event.data) as K8sObject[];

        this.data[url] = this.data[url].concat(page);
        this.subjects[url].next(this.data[url]);
      },
    );
  }

  private handleUpdateEvent(url): Subscription {
    return fromEvent(this.eventSources[url], SSE_WATCH_EVENT.UPDATE).subscribe(
      (event: MessageEvent) => {
        const ev = JSON.parse(event.data) as WatchEvent;

        if (ev.type === WATCH_EVENT_TYPE.ADDED) {
          this.data[url] = this.addObject(this.data[url], ev.object);
        }

        if (ev.type === WATCH_EVENT_TYPE.MODIFIED) {
          this.data[url] = this.updateObject(this.data[url], ev.object);
        }

        if (ev.type === WATCH_EVENT_TYPE.DELETED) {
          this.data[url] = this.deleteObject(this.data[url], ev.object);
        }

        this.subjects[url].next(this.data[url]);
      },
    );
  }

  /**
   * Perform a K8s watch via an SSE connection
   * @param url - The URL to setup an SSE EventSource from
   */
  customResources(url: string): BehaviorSubject<K8sObject[]> {
    if (url in this.eventSources) {
      console.log(`${url}: Using data from already existing SSE connection`);
      return this.subjects[url];
    }

    console.log(`---`);
    console.log(`${url}: Initializing SSE connection`);

    this.eventSources[url] = new EventSource(url, { withCredentials: true });
    this.eventSubs[url] = new Subscription();
    this.data[url] = [];
    this.subjects[url] = new BehaviorSubject<K8sObject[]>([]);

    // event handler subscriptions
    this.eventSubs[url].add(this.handleListEvent(url));
    this.eventSubs[url].add(this.handlePageEvent(url));
    this.eventSubs[url].add(this.handleUpdateEvent(url));

    console.log(`${url}: Setted up SSE watch operation for`);
    console.log(`---`);

    return this.subjects[url];
  }

  /**
   * Stop an SSE watch stream and complete the corresponding Subject
   * @param url - The URL that used to start an SSE stream
   */
  stop(url: string) {
    console.log(`---`);
    console.log(`${url}: Stopping watch operation`);

    if (url in this.eventSources) {
      console.log(`${url}: Closing browser event source`);
      this.eventSources[url].close();
      delete this.eventSources[url];
    }

    if (url in this.eventSubs) {
      console.log(`${url}: Closing subscriptions`);
      this.eventSubs[url].unsubscribe();
      delete this.eventSubs[url];
    }

    if (url in this.data) {
      console.log(`${url}: Removing data`);
      delete this.data[url];
    }

    if (url in this.subjects) {
      console.log(`${url}: Removing rxjs subject`);
      this.subjects[url].complete();
      delete this.subjects[url];
    }

    console.log(`${url}: Stopped watch operation`);
    console.log(`---`);
  }
}
