import { TestBed } from '@angular/core/testing';

import { WatchService } from './watch.service';

describe('WatchService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: WatchService = TestBed.get(WatchService);
    expect(service).toBeTruthy();
  });
});
